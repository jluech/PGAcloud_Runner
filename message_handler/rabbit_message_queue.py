import json
import logging

import pika

from message_handler.message_handler import MessageHandler
from population.individual import IndividualDecoder, IndividualEncoder
from utilities import utils

QUEUE_NAME = "generation"  # TODO: retrieve each time from utils.get_messaging_source()


def receive_evaluated_individuals_callback(channel, method, properties, body):
    payload_dict = json.loads(body.decode("utf-8"), cls=IndividualDecoder)
    population = payload_dict.get("payload")
    logging.info(body)  # TODO: remove
    logging.info(payload_dict)  # TODO: remove
    logging.info("rMQ:{queue_}: Received evaluated individuals: {pop_}".format(
        queue_=QUEUE_NAME,
        pop_=population,
    ))

    received_all = utils.save_received_individual(body)
    if received_all:
        channel.stop_consuming()
        logging.info("rMQ:{queue_}: Stopped consuming.".format(
            queue_=QUEUE_NAME,
        ))


def send_message_to_queue(channel, destinations, payload):
    # This will create the exchange if it doesn't already exist.
    next_recipient = destinations.pop(0)
    channel.queue_declare(queue=next_recipient, auto_delete=True, durable=True)

    # Send message to given recipient.
    logging.info("rMQ: Sending {body_} to destinations {dest_}.".format(
        body_=payload,
        dest_=destinations,
    ))
    channel.basic_publish(
        exchange="",
        routing_key=next_recipient,
        body=json.dumps({
            "destinations": destinations,
            "payload": payload
        }, cls=IndividualEncoder),
        # Delivery mode 2 makes the broker save the message to disk.
        # This will ensure that the message be restored on reboot even
        # if RabbitMQ crashes before having forwarded the message.
        properties=pika.BasicProperties(
            delivery_mode=2,
        ),
    )


class RabbitMessageQueue(MessageHandler):
    def __init__(self, pga_id):
        # Establish connection to rabbitMQ.
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host="rabbitMQ--{id_}".format(id_=pga_id),
            socket_timeout=30,
        ))

    def receive_messages(self):
        # Define communication channel.
        channel = self.connection.channel()

        # Create queue for returning individuals as end of one generation.
        channel.queue_declare(queue=QUEUE_NAME, auto_delete=True, durable=True)

        # Actively listen for messages in queue and perform callback on receive.
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=receive_evaluated_individuals_callback,
            auto_ack=True
        )
        logging.info("rMQ:{queue_}: Waiting for generation individuals feedback.".format(
            queue_=QUEUE_NAME
        ))
        channel.start_consuming()

        # Close connection when finished. TODO: check if prematurely closing connection
        self.connection.close()

    def send_message(self, individual, remaining_destinations):
        # Define communication channel.
        channel = self.connection.channel()
        send_message_to_queue(
            channel=channel,
            destinations=remaining_destinations,
            payload=individual
        )

    def send_broadcast_to_init(self, amount, next_destinations):
        # Define communication channel.
        channel = self.connection.channel()

        # Create the exchange if it doesn't exist already.
        exchange_name = next_destinations.pop(0)
        channel.exchange_declare(exchange=exchange_name, exchange_type="fanout", auto_delete=True, durable=True)

        # Send message to given recipient.
        logging.info("rMQ: Sending '{body_}' to '{init_}' with destinations {dest_}.".format(
            body_=amount,
            init_=exchange_name,
            dest_=next_destinations,
        ))
        channel.basic_publish(
            exchange=exchange_name,
            routing_key="",
            body=json.dumps({
                "destinations": next_destinations,
                "payload": amount
            }),
            # Delivery mode 2 makes the broker save the message to disk.
            # This will ensure that the message be restored on reboot even
            # if RabbitMQ crashes before having forwarded the message.
            properties=pika.BasicProperties(
                delivery_mode=2,
            ),
        )
