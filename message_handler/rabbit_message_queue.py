import json
import logging

import pika

from message_handler.message_handler import MessageHandler
from population.individual import Individual, IndividualEncoder
from utilities import utils


def receive_evaluated_individuals_callback(channel, method, properties, body):
    queue_name = utils.get_messaging_source()

    ind_dict = json.loads(body)
    individual = Individual(ind_dict["solution"], ind_dict["fitness"])

    received_all, individual_number = utils.save_received_individual(individual)
    logging.info("rMQ:{queue_}: Received evaluated individual #{nr_}: {ind_}".format(
        queue_=queue_name,
        nr_=individual_number,
        ind_=individual,
    ))

    if received_all:
        channel.stop_consuming()
        logging.info("rMQ:{queue_}: Stopped consuming.".format(
            queue_=queue_name,
        ))


def send_message_to_queue(channel, payload, next_recipient):
    # This will create the exchange if it doesn't already exist.
    channel.queue_declare(queue=next_recipient, auto_delete=True, durable=True)

    # Send message to given recipient.
    logging.info("rMQ: Sending {body_} to {dest_}.".format(
        body_=payload,
        dest_=next_recipient,
    ))
    channel.basic_publish(
        exchange="",
        routing_key=next_recipient,
        body=json.dumps(payload, cls=IndividualEncoder),
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
        queue_name = utils.get_messaging_source()
        channel.queue_declare(queue=queue_name, auto_delete=True, durable=True)

        # Actively listen for messages in queue and perform callback on receive.
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=receive_evaluated_individuals_callback,
            auto_ack=True
        )
        logging.info("rMQ:{queue_}: Waiting for generation individuals feedback.".format(
            queue_=queue_name
        ))
        channel.start_consuming()

    def send_message(self, individuals, next_recipient):
        # Define communication channel.
        channel = self.connection.channel()
        send_message_to_queue(
            channel=channel,
            payload=individuals,
            next_recipient=next_recipient,
        )

    def send_multiple_to_init(self, individuals_amount, nodes_amount):
        # Define communication channel.
        channel = self.connection.channel()

        # Create the queue if it doesn't exist already.
        queue_name = utils.get_messaging_init_gen()
        channel.queue_declare(queue=queue_name, auto_delete=True, durable=True)

        # Send message to given recipient.
        logging.info("rMQ: Sending '{body_}' to '{init_}' for {nodes_} nodes.".format(
            body_=individuals_amount,
            init_=queue_name,
            nodes_=nodes_amount,
        ))
        for i in range(0, nodes_amount):
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(individuals_amount),
                # Delivery mode 2 makes the broker save the message to disk.
                # This will ensure that the message be restored on reboot even
                # if RabbitMQ crashes before having forwarded the message.
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ),
            )
