import logging
import math

from flask import Flask, make_response, jsonify

from utilities import docker_utils, utils

logging.basicConfig(level=logging.DEBUG)  # TODO: remove and reduce to INFO


# App initialization.
rnr = Flask(__name__)


@rnr.route("/status", methods=["GET"])
def status():
    return "OK"


@rnr.route("/<int:pga_id>/properties", methods=["PUT"])
def init_properties(pga_id):
    # TODO 106: need to transmit properties with request or is docker config enough?
    properties_dict = utils.parse_yaml("/{id_}{sep_}config.yml".format(
                id_=pga_id,
                sep_=docker_utils.PGA_NAME_SEPARATOR
            )
        ).get("properties")
    logging.debug("Distributing properties: {props_}".format(props_=properties_dict))
    # TODO 106: store properties in DB


@rnr.route("/<int:pga_id>/population", methods=["POST"])
def init_population(pga_id):
    config_dict = utils.parse_yaml("/{id_}{sep_}config.yml".format(
        id_=pga_id,
        sep_=docker_utils.PGA_NAME_SEPARATOR
    ))

    generate_population = not config_dict.get("population").get("use_initial_population")
    logging.debug("Initializing population: {init_}".format(init_=generate_population))

    if generate_population:
        total_pop_size = config_dict.get("properties").get("POPULATION_SIZE")
        init_nodes_amount = config_dict.get("setups").get("INIT").get("scaling")
        split_amount = math.ceil(total_pop_size / init_nodes_amount)
        # generate at least as many individuals as required
        # if population size exceeds the POPULATION_SIZE property, the population will be cropped when starting the PGA

        logging.debug("Delegating generation of {size_} individuals to {nodes_} nodes "
                      "with {split_} individuals per node.".format(
                        size_=total_pop_size,
                        nodes_=init_nodes_amount,
                        split_=split_amount
                        ))
        # TODO 106: send message with amount to generate per INIT to INIT rMQ (broadcast mode)
    else:
        # TODO 106: assign and store population
        # TODO 106: package population into individuals
        # TODO 106: send individuals to FE rMQ (queue mode)
        pass
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


if __name__ == "__main__":
    rnr.run(host="0.0.0.0", debug=True)  # TODO: remove debug mode
