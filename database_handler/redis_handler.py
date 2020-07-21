import redis

from database_handler.database_handler import DatabaseHandler


class RedisHandler(DatabaseHandler):
    def __init__(self, pga_id):
        self.redis = redis.Redis(host="redis--{id_}".format(id_=pga_id))

    def store(self, properties_dict):
        prop_keys = [*properties_dict]
        for prop_key in prop_keys:
            value = properties_dict[prop_key]
            if not type(value) in [str, int]:
                value = str(value)
            self.redis.set(prop_key, value)

    def retrieve(self, property_name):
        return self.redis.get(property_name)
