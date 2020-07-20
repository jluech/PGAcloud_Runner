from abc import ABC, abstractmethod


class MessageHandler(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def receive_messages(self):
        pass

    @abstractmethod
    def send_message(self, payload, remaining_destinations):
        # remaining_destinations is a list of strings with remaining recipients, in order of reception
        pass
