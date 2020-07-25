import yaml

from population.pair import Pair

PGA_NAME_SEPARATOR = "--"
__PROPERTIES = {}
__EVALUATED_INDIVIDUALS = []


def parse_yaml(yaml_file_path):
    with open(yaml_file_path, mode="r", encoding="utf-8") as yaml_file:
        content = yaml.safe_load(yaml_file) or {}
    return content


def collect_and_reset_received_individuals():
    global __EVALUATED_INDIVIDUALS
    received = __EVALUATED_INDIVIDUALS
    __EVALUATED_INDIVIDUALS = []
    return received


def save_received_individual(individual):
    global __EVALUATED_INDIVIDUALS
    __EVALUATED_INDIVIDUALS.append(individual)
    return __EVALUATED_INDIVIDUALS.__len__() >= int(get_property("POPULATION_SIZE"))


def sort_population_by_fitness(population):
    # Sorts and returns population by fitness, in descending order (fittest first).
    return population.sort(key=lambda ind: ind.fitness, reverse=True)


def split_population_into_pairs(population):
    # Splits the population and returns an array of Pair's.
    half_point = population.__len__() / 2
    if half_point < 1:
        raise Exception("Cannot split array with less than two items into Pair's!")

    first = population[:half_point]
    second = population[half_point:]
    if half_point % 2 > 0:
        first.append(population[-1])

    pairs = []
    for i in range(half_point):
        pairs.append(Pair(first[i], second[i]))
    return pairs


def get_property(property_key):
    return __PROPERTIES[property_key]


def set_property(property_key, property_value):
    __PROPERTIES[property_key] = property_value
