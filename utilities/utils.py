import yaml

from population.pair import Pair


def parse_yaml(yaml_file_path):
    with open(yaml_file_path, mode="r", encoding="utf-8") as yaml_file:
        content = yaml.safe_load(yaml_file) or {}
    return content


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
