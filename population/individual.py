class Individual(object):
    def __init__(self, solution, fitness=None):
        self.solution = solution
        self.fitness = fitness if fitness else 0
