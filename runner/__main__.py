import logging
import math

from flask import Flask, make_response, jsonify

from database_handler.handlers import DatabaseHandlers
from database_handler.redis_handler import RedisHandler
from message_handler.handlers import MessageHandlers
from message_handler.rabbit_message_queue import RabbitMessageQueue
from utilities import docker_utils, utils

logging.basicConfig(level=logging.DEBUG)  # TODO: remove and reduce to INFO

DATABASE_HANDLER = DatabaseHandlers.Redis
MESSAGE_HANDLER = MessageHandlers.RabbitMQ


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
                sep_=docker_utils.PGA_NAME_SEPARATOR
            )
        ).get("properties")

    logging.debug("Appending pga_id {id_} to properties.".format(id_=pga_id))
    properties_dict["PGAcloud_pga_id"] = pga_id

    logging.debug("Distributing properties: {props_}".format(props_=properties_dict))

    # Store properties in database for retrieval of other components.
    database = get_database_handler(pga_id)
    database.store(properties_dict)

    # Log confirmation.
    stored_properties = {}
    for key in [*properties_dict]:
        stored_properties[key] = database.retrieve(key)
    logging.debug("Successfully stored properties: {stored_}".format(stored_=stored_properties))

    return make_response(jsonify(None), 204)


@rnr.route("/<int:pga_id>/population", methods=["POST"])
def init_population(pga_id):
    config_dict = utils.parse_yaml("/{id_}{sep_}config.yml".format(
        id_=pga_id,
        sep_=docker_utils.PGA_NAME_SEPARATOR
    ))

    generate_population = not config_dict.get("population").get("use_initial_population")
    logging.debug("Initializing population: {init_}".format(init_=generate_population))

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

        logging.debug("Delegating generating {size_} individuals to {nodes_} nodes "
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

    return make_response(jsonify(None), 204)


@rnr.route("/<int:pga_id>/start", methods=["PUT"])
def start_pga(pga_id):
    # init()
    # get termination criteria
    # assign generations_done counter = 0
    # assign start time
    # assign unchanged generations = 0
    #
    #
    # run()
    # listen to FE rMQ
    # compare size of population against POP_SIZE property - error if too small, crop if too large
    # while not terminated:
    # assign and store population
    # check for termination - if not done then proceed, else stop and report
    # apply elitism and separate fittest
    # package remaining population into individuals
    # send individuals to SEL rMQ (queue mode)
    # listen to FE queue
    # combine elite and new population
    # apply survival selection
    # increase generations_done counter
    #
    #
    # stop()
    # assign and store population
    # call MGR to report fittest individual(s) and remove operators
    # this will leave the RUN and DB so that a user could search the DB

    # Retrieve configuration.
    # if
    # config =

    # Collect termination criteria.
    max_generations = 1500
    max_unchanged_generations = 300
    max_time_seconds = 600
    population_size = 100


def run_pga():
    pass


def stop_pga():
    pass


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
