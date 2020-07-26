from json import JSONEncoder, JSONDecoder

DEFAULT_FITNESS = 0


class Individual(object):
    def __init__(self, solution, fitness=None):
        self.solution = solution
        self.fitness = fitness if fitness else DEFAULT_FITNESS


# Make class JSON serializable:
# https://pynative.com/make-python-class-json-serializable/
class IndividualEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__


class IndividualDecoder(JSONDecoder):
    def default(self, obj):
        return Individual(obj["solution"], obj["fitness"])
