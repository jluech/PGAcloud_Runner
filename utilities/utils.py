import logging
import math
import operator
import os
from re import match

import yaml

PGA_NAME_SEPARATOR = "--"
__CONTAINER_CONF = None
__PROPERTIES = {}
__EVALUATED_INDIVIDUALS = []


# YAML command
def parse_yaml(yaml_file_path):
    with open(yaml_file_path, mode="r", encoding="utf-8") as yaml_file:
        content = yaml.safe_load(yaml_file) or {}
    return content


# Commands for population and individuals
def collect_and_reset_received_individuals():
    global __EVALUATED_INDIVIDUALS
    received = sort_population_by_fitness(__EVALUATED_INDIVIDUALS)
    __EVALUATED_INDIVIDUALS = []
    return received


def save_received_individual(individual):
    global __EVALUATED_INDIVIDUALS
    __EVALUATED_INDIVIDUALS.append(individual)
    current_length = __EVALUATED_INDIVIDUALS.__len__()
    return current_length >= int(get_property("POPULATION_SIZE")), current_length


def sort_population_by_fitness(population):
    # Sorts and returns population by fitness, in descending order (fittest first).
    return sorted(population, key=operator.attrgetter("fitness"), reverse=True)


def split_population_into_pairs(population):
    # Splits the population and returns an array of Individual pairs.
    half_point = population.__len__() / 2
    if half_point < 1:
        raise Exception("Cannot split array with less than two items into Pair's!")

    logging.info("Splitting population.")

    first = population[:half_point]
    second = population[half_point:]
    if half_point % 2 > 0:
        first.append(population[-1])

    logging.info(first)  # TODO: remove
    logging.info(second)

    pairs = []
    for i in range(half_point):
        pairs.append([first[i], second[i]])
    return pairs


# Commands for properties
def get_messaging_source():
    if not __CONTAINER_CONF:
        __retrieve_container_config()
    return __CONTAINER_CONF["source"]


def get_messaging_init_gen():
    if not __CONTAINER_CONF:
        __retrieve_container_config()
    return __CONTAINER_CONF["init_gen"]


def get_messaging_init_eval():
    if not __CONTAINER_CONF:
        __retrieve_container_config()
    return __CONTAINER_CONF["init_eval"]


def get_messaging_pga():
    if not __CONTAINER_CONF:
        __retrieve_container_config()
    return __CONTAINER_CONF["pga"]


def get_pga_id():
    if not __CONTAINER_CONF:
        __retrieve_container_config()
    return __CONTAINER_CONF["pga_id"]


def __retrieve_container_config():
    # Retrieve locally saved config file.
    files = [f for f in os.listdir("/") if match(r'[0-9]+--runner-config\.yml', f)]
    # https://stackoverflow.com/questions/2225564/get-a-filtered-list-of-files-in-a-directory/2225927#2225927
    # https://regex101.com/

    if not files.__len__() > 0:
        raise Exception("Error retrieving the container config: No matching config file found!")
    config = parse_yaml("/{}".format(files[0]))
    global __CONTAINER_CONF
    __CONTAINER_CONF = {
        "pga_id": config.get("pga_id"),
        "source": config.get("source"),
        "init_gen": config.get("init_gen"),
        "init_eval": config.get("init_eval"),
        "pga": config.get("pga")
    }
    logging.info("Container config retrieved: {conf_}".format(conf_=__CONTAINER_CONF))


def get_property(property_key):
    return __PROPERTIES[property_key]


def set_property(property_key, property_value):
    __PROPERTIES[property_key] = property_value
