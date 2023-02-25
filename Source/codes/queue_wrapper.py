# This script is going to take in wrapper class/classes designed for communication with rabbitmq & masstransit 
# The script is aiming at adding more transparency between processing portion and the communication part

from pika import PlainCredentials
from pika import BlockingConnection
from pika import ConnectionParameters
from masstransitpython import RabbitMQConfiguration
# from masstransitpython import RabbitMQReceiver
from masstransitpython import RabbitMQSender
from pika import BlockingConnection
from pika import ConnectionParameters
from threading import Thread
from json import loads, JSONEncoder
# import pandas as pd
import json
from uuid import UUID
import os
import logging


RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_PORT = os.environ['RABBITMQ_PORT']
RABBITMQ_VIRTUAL_HOST = os.environ['RABBITMQ_VIRTUAL_HOST']
VERBOSE = os.environ['VERBOSE']

CORE_CONSUME_QUEUE = os.environ['CORE_CONSUME_QUEUE']
RAPID_CONSUME_QUEUE = os.environ['RAPID_CONSUME_QUEUE']
UTIL_CONSUME_QUEUE = os.environ['UTIL_CONSUME_QUEUE']
PUBLISH_QUEUE = os.environ['PUBLISH_QUEUE']


class RabbitMQReceiver():
    """
        This class overwrites the original class from the library
        by eliminating the singleton feature which I absolutely don't 
        comprehend why it developed in the first place!
    """
    __slots__ = ["_configuration", "_connection", "_channel", "_queue", "_routing_key", "_exchange",
                 "_on_message_callback"]

    def __init__(self, configuration, exchange, routing_key=''):
        """
        Create RabbitMQ Sender
        :param configuration: RabbitMQConfiguration object
        """
        self._configuration = configuration
        self._connection = BlockingConnection(ConnectionParameters(host=self._configuration.host,
                                                                   port=self._configuration.port,
                                                                   virtual_host=self._configuration.virtual_host,
                                                                   credentials=self._configuration.credentials))
        self._channel = self._connection.channel()
        self._queue = self._configuration.queue
        self._routing_key = routing_key
        self._exchange = exchange
        self._channel.queue_declare(queue=self._queue)
        self._channel.exchange_declare(exchange=exchange,
                                       exchange_type='fanout',
                                       durable=True)
        self._channel.queue_bind(queue=self._queue,
                                 exchange=self._exchange,
                                 routing_key=self._routing_key)
        self._on_message_callback = None

    def add_on_message_callback(self, on_message_callback):
        """
        Add function callback
        :param self:
        :param on_message_callback: function where the message is consumed
        :return: None
        """
        self._on_message_callback = on_message_callback

    def start_consuming(self):
        """ Start consumer with earlier defined callback """
        logging.info(f"Listening to {self._queue} queue\n")
        self._channel.basic_consume(queue=self._queue,
                                    on_message_callback=self._on_message_callback,
                                    auto_ack=True)
        self._channel.start_consuming()


class DurableRabbitMQReceiver(RabbitMQReceiver):
    """
        This class instantiates and configures a durable rabbit receiver. 
        The listener/Sender class should be compatible with the exchange/queue settings. 
    """

    def __init__(self, configuration, exchange, routing_key=''):
        """
        Create RabbitMQ Sender
        :param configuration: RabbitMQConfiguration object
        """
        
        self._configuration = configuration
        self._connection = BlockingConnection(ConnectionParameters(heartbeat=0,
                                                                   host=self._configuration.host,
                                                                   port=self._configuration.port,
                                                                   virtual_host=self._configuration.virtual_host,
                                                                   credentials=self._configuration.credentials))
        
        self._channel = self._connection.channel()
        self._queue = self._configuration.queue
        self._routing_key = routing_key
        self._exchange = exchange
        self._channel.queue_declare(queue=self._queue, durable=True, 
                                    arguments= {'x-queue-mode':'lazy'})
        
        self._channel.exchange_declare(exchange=exchange,
                                       exchange_type='fanout',
                                       durable=True)
        
        self._channel.queue_bind(queue=self._queue,
                                 exchange=self._exchange,
                                 routing_key=self._routing_key)
        
        self._on_message_callback = None
    
    def start_consuming(self):
        """ Start consumer with earlier defined callback """
        logging.info(f"Listening to {self._queue} queue\n")
        self._channel.basic_consume(queue=self._queue,
                                    on_message_callback=self._on_message_callback,
                                    auto_ack=True)
        self._channel.start_consuming()

class DurableRabbitMQSender(RabbitMQSender):
    """
        This class instantiates and configures a durable rabbit sender. 
        The listener/Sender class should be compatible with the exchange/queue settings. 
    """
 
    def __init__(self, configuration):
        """
        Create RabbitMQ Sender
        :param configuration: RabbitMQConfiguration object
        """
        self._configuration = configuration
        self._connection = BlockingConnection(ConnectionParameters(heartbeat=0,
                                                                   host=self._configuration.host,
                                                                   port=self._configuration.port,
                                                                   virtual_host=self._configuration.virtual_host,
                                                                   credentials=self._configuration.credentials))
        self._channel = self._connection.channel()
        self._queue = self._configuration.queue
        self._channel.queue_declare(queue=self._queue, durable=True, 
                                    arguments= {'x-queue-mode':'lazy'})
        self._routing_key = ''
        self._exchange = ''

    def create_masstransit_response(self, message, request_body):

        response = {

              "messageId": request_body['messageId'],

              "conversationId": request_body['conversationId'],

              "sourceAddress": request_body['sourceAddress'],

              "destinationAddress": request_body['destinationAddress'],

              "messageType": [
                "urn:message:SMP.RabbitMQ.Bus.TransferClass:ReceivingContent"
              ],

              "message": message,

        }
        return json.dumps(response, cls=UUIDEncoder)

class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)

############################################
class TheVeryFirstWrapperClass():
    def __init__(self):
        # to implement
        print('wrapper initiation.')

    def pick_up_column(self):
        # to implement
        print('picking up and returning a column')
        df = pd.read_excel(r'C:\Users\m.jafari\Desktop\datasets\01 MCI_6month_bills_p\sample.xlsx')
        months = df.iloc[:, 6]
        print(months)
        return months
    
    def pick_up_whole(self):
        # to implement
        print('picking up and returning a column')
        df = pd.read_excel(r'C:\Users\m.jafari\Desktop\datasets\01 MCI_6month_bills_p\sample.xlsx')
        return df

    def send_message(self, df):
        # to implement
        print('enqueuing a processed item')

    def consume(self):
        print('method has been desigend to take responsibility of handling enqueued items one-by-one')

###########################################




