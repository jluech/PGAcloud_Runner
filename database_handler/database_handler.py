from abc import ABC, abstractmethod


class DatabaseHandler(ABC):
    @abstractmethod
    def __init__(self, pga_id):
        # pga_id required to identify the specific database service
        pass

    @abstractmethod
    def store(self, properties_list):
        pass

    @abstractmethod
    def retrieve(self, property_name):
        pass
