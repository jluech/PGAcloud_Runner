from abc import ABC, abstractmethod


class DatabaseHandler(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def store(self):
        pass

    @abstractmethod
    def retrieve(self):
        pass
