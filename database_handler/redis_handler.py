import json
import logging

import redis

from database_handler.database_handler import DatabaseHandler
from population.individual import IndividualEncoder


class RedisHandler(DatabaseHandler):
    def __init__(self, pga_id):
        self.redis = redis.Redis(host="redis--{id_}".format(id_=pga_id))

    def store_properties(self, properties_dict):
        prop_keys = [*properties_dict]
        for prop_key in prop_keys:
            value = properties_dict[prop_key]
            if not type(value) in [str, int, list]:
                value = str(value)
            logging.info("redis: Storing property '{prop_}'={val_}".format(
                prop_=prop_key,
                val_=value,
            ))
            if type(value) is list:
                for val in value:
                    self.redis.lpush(prop_key, val)
            else:
                self.redis.set(prop_key, value)

    def store_population(self, population):
        logging.info("redis: Storing population.")
        for individual in population:
            serialized_individual = json.dumps(individual, cls=IndividualEncoder)
            self.redis.lpush("population", serialized_individual)

    def retrieve_item(self, property_name):
        return self.redis.get(property_name)

    def retrieve_list(self, property_name):
        return self.redis.lrange(property_name, 0, -1)
