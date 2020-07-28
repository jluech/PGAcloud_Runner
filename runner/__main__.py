import json
import logging
import math
import time
import warnings

from flask import Flask, make_response, jsonify

from database_handler.handlers import DatabaseHandlers
from database_handler.redis_handler import RedisHandler
from message_handler.handlers import MessageHandlers
from message_handler.rabbit_message_queue import RabbitMessageQueue
from population.individual import Individual, IndividualEncoder
from utilities import utils

logging.basicConfig(level=logging.INFO)

DATABASE_HANDLER = DatabaseHandlers.Redis
MESSAGE_HANDLER = MessageHandlers.RabbitMQ
RELEVANT_PROPERTIES = ["POPULATION_SIZE", "ELITISM_RATE"]

__ABORTING = False


# App initialization.
rnr = Flask(__name__)


@rnr.route("/status", methods=["GET"])
def status():
    return "OK"


@rnr.route("/<int:pga_id>/properties", methods=["PUT"])
def init_properties(pga_id):
    # TODO 106: need to transmit properties with request or is docker config enough?
    # Prepare properties to store.
    properties_dict = utils.parse_yaml("/{id_}{sep_}config.yml".format(
                id_=pga_id,
                sep_=utils.PGA_NAME_SEPARATOR
            )
        ).get("properties")

    logging.info("Appending pga_id {id_} to properties.".format(id_=pga_id))
    properties_dict["PGAcloud_pga_id"] = pga_id

    logging.info("Distributing properties: {props_}".format(props_=properties_dict))

    # Store properties in database for retrieval of other components.
    database = get_database_handler(pga_id)
    database.store_properties(properties_dict)

    # Log confirmation.
    stored_properties = {}
    for key in [*properties_dict]:
        stored_properties[key] = database.retrieve(key)
    logging.info("Successfully stored properties: {stored_}".format(stored_=stored_properties))

    return make_response(jsonify(None), 204)


@rnr.route("/<int:pga_id>/population", methods=["POST"])
def init_population(pga_id):
    config_dict = utils.parse_yaml("/{id_}{sep_}config.yml".format(
        id_=pga_id,
        sep_=utils.PGA_NAME_SEPARATOR
    ))

    use_initial_population = config_dict.get("population").get("use_initial_population")
    generate_population = False
    if not use_initial_population:
        generate_population = True
    logging.info("Initializing population: {init_}".format(init_=generate_population))

    # Retrieve first recipients: initialization is index 0 to 2, rest is pga chain
    message_handler = get_message_handler(pga_id)

    if generate_population:
        total_pop_size = config_dict.get("properties").get("POPULATION_SIZE")
        init_nodes_amount = config_dict.get("setups").get("INIT").get("scaling")
        split_amount = math.ceil(total_pop_size / init_nodes_amount)
        # generate at least as many individuals as required
        # if population size exceeds the POPULATION_SIZE property, the population will be cropped when starting the PGA

        logging.info("Delegating generating {size_} individuals to {nodes_} nodes "
                     "with {split_} individuals per node.".format(
                        size_=total_pop_size,
                        nodes_=init_nodes_amount,
                        split_=split_amount
                        ))
        message_handler.send_multiple_to_init(individuals_amount=split_amount, nodes_amount=init_nodes_amount)
    else:
        # Read and parse provided population.
        population_path = config_dict.get("population").get("population_file_path")
        solutions = utils.parse_yaml(population_path)
        logging.info(solutions)  # TODO: remove
        population = []
        for solution in solutions:
            population.append(Individual(solution))

        # Send individuals to fitness evaluation.
        next_recipient = utils.get_messaging_init_eval()
        pairs = utils.split_population_into_pairs(population)
        for pair in pairs:
            message_handler.send_message(individuals=pair, next_recipient=next_recipient)

        # Store current population.
        database_handler = get_database_handler(pga_id)
        database_handler.store_population(population)

    return make_response(jsonify(None), 201)


@rnr.route("/<int:pga_id>/start", methods=["PUT"])
def start_pga(pga_id):
    population = run_pga(pga_id)
    fittest = stop_pga(pga_id, population)

    return make_response(jsonify({
        "id": pga_id,
        "fittest": json.dumps(fittest, cls=IndividualEncoder)
    }), 204)


@rnr.route("/stop")
def abort_pga():
    global __ABORTING
    __ABORTING = True
    return make_response(jsonify(None), 202)


def run_pga(pga_id):
    # Get support handlers.
    database_handler = get_database_handler(pga_id)
    message_handler = get_message_handler(pga_id)

    # Collect termination criteria.
    config_dict = utils.parse_yaml("/{id_}{sep_}config.yml".format(
        id_=pga_id,
        sep_=utils.PGA_NAME_SEPARATOR
    ))
    max_generations = config_dict.get("properties").get("MAX_GENERATIONS")
    max_unimproved_generations = config_dict.get("properties").get("MAX_UNIMPROVED_GENERATIONS")
    max_time_seconds = config_dict.get("properties").get("MAX_TIME_SECONDS")

    # Set relevant properties.
    for prop in RELEVANT_PROPERTIES:  # TODO: retrieve relevant list from container config file?
        value = database_handler.retrieve(prop)
        utils.set_property(prop, value)
    elitism_rate = float(utils.get_property("ELITISM_RATE"))

    # Initialize population and settings.
    logging.info("Collecting evaluated initial population.")
    message_handler.receive_messages()
    population = utils.collect_and_reset_received_individuals()

    # Crop population if too large.
    population_size = int(utils.get_property("POPULATION_SIZE"))
    if population.__len__() > population_size:
        warnings.warn("Population too large! Expected {exp_} - Actual {act_}".format(
            exp_=population_size,
            act_=population.__len__()
        ))
        logging.info("Cropping population to defined size.")
        population = population[:population_size]
    elif population.__len__() < population_size:
        warnings.warn("Population not large enough! Expected {exp_} - Actual {act_}".format(
            exp_=population_size,
            act_=population.__len__()
        ))

    # Prepare generation handling.
    generations_done = 0
    unimproved_generations = 0
    pga_runtime = 0
    pga_start_time = time.perf_counter()

    # Run generations.
    while (generations_done < max_generations
           and unimproved_generations < max_unimproved_generations
           and pga_runtime < max_time_seconds):
        logging.info("Starting new generation: {gen_}".format(gen_=generations_done+1))

        # Store population in database.
        database_handler.store_population(population)
        old_fittest = population[0]

        # Check if an abort request was issued.
        if __ABORTING:
            logging.info("ATTENTION: Aborting PGA!")
            break

        # Apply elitism.
        logging.info("Applying elitism to current population.")
        elite_portion = math.floor(population.__len__() * elitism_rate)
        elite = population[:elite_portion]

        # Package population into individuals and release to model.
        pairs = utils.split_population_into_pairs(population)
        logging.info("Releasing population to model.")
        next_recipient = utils.get_messaging_pga()
        for pair in pairs:
            message_handler.send_message(pair, next_recipient)

        # Listen to FE queue.
        message_handler.receive_messages()
        new_individuals = utils.collect_and_reset_received_individuals()

        # Crop new population if too large.
        if new_individuals.__len__() > population_size:
            logging.info("Cropping oversized population! Expected {exp_} - Actual {act_}".format(
                exp_=population_size,
                act_=new_individuals.__len__()
            ))
            new_individuals = new_individuals[:population_size]
        elif new_individuals.__len__() < population_size:
            warnings.warn("Population not large enough! Expected {exp_} - Actual {act_}".format(
                exp_=population_size,
                act_=new_individuals.__len__()
            ))

        # Combine elite and returning individuals, sort by fitness.
        logging.info("Sorting population.")
        sorted_population = utils.sort_population_by_fitness(elite + new_individuals)

        # Apply survival selection.
        logging.info("Applying survival selection to population.")
        population = select_survivors(sorted_population, elite_portion)

        # Finish generation.
        generations_done += 1
        if old_fittest.fitness >= population[0].fitness:
            unimproved_generations += 1
            logging.info("Finished generation #{gen_} - unimproved #{unimp_}.".format(
                gen_=generations_done,
                unimp_=unimproved_generations,
            ))
        else:
            unimproved_generations = 0
            logging.info("Finished generation #{gen_} - improved individuals.".format(gen_=generations_done))
        pga_runtime = time.perf_counter() - pga_start_time

    return population


def stop_pga(pga_id, population):
    # Get support handlers.
    database_handler = get_database_handler(pga_id)

    # Store population and determine fittest individual.
    sorted_population = utils.sort_population_by_fitness(population)
    logging.info("Storing final population with {amount_} individuals.".format(amount_=sorted_population.__len__()))
    database_handler.store_population(sorted_population)
    fittest = sorted_population[0]

    logging.info("Terminating PGA. Fittest individual: fit={fit_}, sol={sol_}".format(
        fit_=fittest.fitness,
        sol_=fittest.solution,
    ))
    return fittest


def select_survivors(sorted_population, kill_portion):
    return sorted_population[:-kill_portion]  # fittest first, so remove at back end.


def get_database_handler(pga_id):
    if DATABASE_HANDLER == DatabaseHandlers.Redis:
        return RedisHandler(pga_id)
    else:
        raise Exception("No valid DatabaseHandler defined!")


def get_message_handler(pga_id):
    if MESSAGE_HANDLER == MessageHandlers.RabbitMQ:
        return RabbitMessageQueue(pga_id)
    else:
        raise Exception("No valid MessageHandler defined!")


if __name__ == "__main__":
    rnr.run(host="0.0.0.0")
