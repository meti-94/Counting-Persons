# This script is going to take in wrapper class/classes designed for communication with rabbitmq & masstransit 
# The script is aiming at adding more transparency between processing portion and the communication part

from pika import PlainCredentials
from masstransitpython import RabbitMQConfiguration
from masstransitpython import RabbitMQReceiver
from masstransitpython import RabbitMQSender
from pika import BlockingConnection
from pika import ConnectionParameters
from threading import Thread
from json import loads, JSONEncoder
# import pandas as pd
import json
from uuid import UUID
import os


RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_PORT = os.environ['RABBITMQ_PORT']
RABBITMQ_VIRTUAL_HOST = os.environ['RABBITMQ_VIRTUAL_HOST']

PUBLISH_QUEUE = os.environ['PUBLISH_QUEUE']
CONSUME_QUEUE = os.environ['CONSUME_QUEUE']

# credentials in plain text is placed here intentionally
# RABBITMQ_USERNAME = 'guest'
# RABBITMQ_PASSWORD = 'HahRa@7554%#'
# RABBITMQ_HOST = '194.5.188.18'
# RABBITMQ_PORT = 8443
# RABBITMQ_VIRTUAL_HOST = '/'

# PUBLISH_QUEUE = 'ReceiveDataProject'
# CONSUME_QUEUE = 'SendDataProject'



class DurableRabbitMQReceiver(RabbitMQReceiver):
    
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
        self._channel.queue_declare(queue=self._queue, durable=True, 
                                    arguments= {'x-queue-mode':'lazy'})
        self._channel.exchange_declare(exchange=exchange,
                                       exchange_type='fanout',
                                       durable=True)
        self._channel.queue_bind(queue=self._queue,
                                 exchange=self._exchange,
                                 routing_key=self._routing_key)
        self._on_message_callback = None

class DurableRabbitMQSender(RabbitMQSender):
    def __init__(self, configuration):
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

                f"urn:message:TestRabbitMq:{PUBLISH_QUEUE}"

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




