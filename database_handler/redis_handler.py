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
            if not type(value) in [str, int]:
                value = str(value)
            logging.info("redis: Storing property '{prop_}'={val_}".format(
                prop_=prop_key,
                val_=value,
            ))
            self.redis.set(prop_key, value)

    def store_population(self, population):
        serialized_population = json.dumps(population, cls=IndividualEncoder)
        logging.info("redis: Storing population {pop_}".format(
            pop_=serialized_population,
        ))
        self.redis.set("population", serialized_population)

    def retrieve(self, property_name):
        return self.redis.get(property_name)
