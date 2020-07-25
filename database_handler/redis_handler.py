import json

import redis

from database_handler.database_handler import DatabaseHandler


class RedisHandler(DatabaseHandler):
    def __init__(self, pga_id):
        self.redis = redis.Redis(host="redis--{id_}".format(id_=pga_id))

    def store(self, population):
        deserialized_property = json.dumps(population)
        self.redis.set("population", deserialized_property)

    def retrieve(self, property_name):
        unpacked_property = json.loads(self.redis.get(property_name).decode('utf-8'))
        return unpacked_property
