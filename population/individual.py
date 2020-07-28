from json import JSONEncoder

DEFAULT_FITNESS = 0.0


class Individual(object):
    def __init__(self, solution, fitness=None):
        self.solution = solution
        self.fitness = fitness if fitness else DEFAULT_FITNESS

    def __repr__(self):
        return '{"solution": ' + self.solution + '; "fitness": ' + str(self.fitness) + '}'


# Make class JSON serializable:
# https://pynative.com/make-python-class-json-serializable/
class IndividualEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__
