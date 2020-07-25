import logging
import math
import time
import warnings

import requests
from flask import Flask, make_response, jsonify

from database_handler.handlers import DatabaseHandlers
from database_handler.redis_handler import RedisHandler
from message_handler.handlers import MessageHandlers
from message_handler.rabbit_message_queue import RabbitMessageQueue
from utilities import utils

logging.basicConfig(level=logging.DEBUG)  # TODO: remove and reduce to INFO

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
    database.store(properties_dict)

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

    generate_population = not config_dict.get("population").get("use_initial_population")
    logging.info("Initializing population: {init_}".format(init_=generate_population))

    message_handler = get_message_handler(pga_id)
    fitness_destination = config_dict.get("operators").get("FE").get("messaging")
    runner_destination = config_dict.get("setups").get("RUN").get("messaging")
    next_destinations = [fitness_destination, runner_destination]

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

        init_destination = config_dict.get("setups").get("INIT").get("messaging")
        next_destinations.insert(0, init_destination)

        message_handler.send_broadcast_to_init(payload=split_amount, destinations=next_destinations)
    else:
        population = []  # TODO 106: read population
        pairs = utils.split_population_into_pairs(population)
        for pair in pairs:
            message_handler.send_message(pair=pair, remaining_destinations=next_destinations)

    return make_response(jsonify(None), 201)


@rnr.route("/<int:pga_id>/start", methods=["PUT"])
def start_pga(pga_id):
    population = run_pga(pga_id)
    stop_pga(pga_id, population)

    return make_response(jsonify({"id": pga_id}), 204)


@rnr.route("/stop")
def abort_pga():
    global __ABORTING
    __ABORTING = True
    return make_response(jsonify(None), 202)


def run_pga(pga_id):
    # Collect termination criteria. TODO: retrieve from config file
    max_generations = 1500
    max_unimproved_generations = 300
    max_time_seconds = 600

    # Get support handlers.
    database_handler = get_database_handler(pga_id)
    message_handler = get_message_handler(pga_id)

    # Set relevant properties.
    for prop in RELEVANT_PROPERTIES:  # TODO: retrieve relevant list from config file
        value = database_handler.retrieve(prop)
        utils.set_property(prop, value)
    elitism_rate = float(utils.get_property("ELITISM_RATE"))

    # Initialize population and settings.
    message_handler.receive_messages()
    population = utils.collect_and_reset_received_individuals()
    population_size = int(utils.get_property("POPULATION_SIZE"))
    # Crop population if too large.
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
    model = []  # TODO: retrieve model and next destinations from config file
    generations_done = 0
    unimproved_generations = 0
    pga_runtime = 0
    pga_start_time = time.perf_counter()

    while (generations_done < max_generations
           and unimproved_generations < max_unimproved_generations
           and pga_runtime < max_time_seconds):
        # Store population in database.
        database_handler.store(population)
        old_fittest = population[0]

        # Check if an abort request was issued.
        if __ABORTING:
            break

        # Apply elitism.
        sorted_population = utils.sort_population_by_fitness(population)
        elite_portion = math.floor(sorted_population.__len__() * elitism_rate)
        elite = sorted_population[:elite_portion]

        # Package population into individuals and release to model.
        individuals = utils.split_population_into_pairs(sorted_population)
        for ind in individuals:
            message_handler.send_message(ind, model)

        # Listen to FE queue.
        message_handler.receive_messages()
        new_individuals = utils.collect_and_reset_received_individuals()

        # Crop new population if too large.
        if new_individuals.__len__() > population_size:
            new_individuals = new_individuals[:population_size]
        elif new_individuals.__len__() < population_size:
            warnings.warn("Population not large enough! Expected {exp_} - Actual {act_}".format(
                exp_=population_size,
                act_=population.__len__()
            ))

        # Combine elite and returning individuals, sort by fitness.
        sorted_population = utils.sort_population_by_fitness(elite + new_individuals)

        # Apply survival selection.
        population = select_survivors(sorted_population, elite_portion)

        # Finish generation.
        generations_done += 1
        if old_fittest.fitness >= population[0].fitness:
            unimproved_generations += 1
        else:
            unimproved_generations = 0
        pga_runtime = time.perf_counter() - pga_start_time

    return population


def stop_pga(pga_id, population):
    # Get support handlers.
    database_handler = get_database_handler(pga_id)

    # Retrieve config file.
    config = {}  # TODO:

    # Store population and determine fittest individual.
    sorted_population = utils.sort_population_by_fitness(population)
    database_handler.store(sorted_population)
    fittest = sorted_population[0]

    # TODO: check if manager is blocking until finished. If so, return fittest in regular /start call
    # if __ABORTING then the fittest will be returned by the /start thread and not by /abort thread

    # Call MGR to report fittest individual and remove operators.
    # This will leave the RUN and DB so that a user could search the DB.
    requests.put(
        url="http://{host_}:{port_}/pga/{id_}/result".format(
            host_=config.get("master_host"),
            port_=config.get("master_port"),
            id_=pga_id
        ),
        params={
            "orchestrator": config.get("orchestrator"),
        },
        data=jsonify({"solution": fittest.solution, "fitness": fittest.fitness}),
        verify=False
    )


def select_survivors(sorted_population, kill_ratio):
    return sorted_population[:-kill_ratio]  # fittest first, so remove at back end.


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
    rnr.run(host="0.0.0.0", debug=True)  # TODO: remove debug mode
