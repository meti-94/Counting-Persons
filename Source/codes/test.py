from pika import PlainCredentials
from masstransitpython import RabbitMQConfiguration
from masstransitpython import RabbitMQReceiver
from masstransitpython import RabbitMQSender
from pika import BlockingConnection
from pika import ConnectionParameters
from threading import Thread
from json import loads, JSONEncoder
import json
from uuid import UUID
import uuid
import os
from PIL import Image
import base64
from io import BytesIO
from time import time
import sys



from queue_wrapper import *
CORE_PUBLISH_QUEUE = os.environ['CORE_CONSUME_QUEUE']
RAPID_CONSUME_QUEUE = os.environ['RAPID_CONSUME_QUEUE']
UTIL_CONSUME_QUEUE = os.environ['UTIL_CONSUME_QUEUE']

global count
global rep

SAMPLE_IMAGE = ["https://iili.io/myctrN.jpg"]*5

                
class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)

class SampleMessage:
    def __init__(self, identifier, data):
        self.id = identifier
        self.data = data


class MessageEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__



if __name__ == "__main__":
    rep = 2
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=CORE_PUBLISH_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    # create sender and send a value
    with DurableRabbitMQSender(sender_conf) as sender:
        sender.set_exchange(CORE_PUBLISH_QUEUE)
        
        for i in range(rep):
            _id = f'CORE_CONSUME_QUEUE{i}'
        
            request_body = {}
            request_body['messageId'] = '0afe66e7-d89e-47e7-964e-153f7afda4b4'
            request_body['conversationId'] = 'ebb00000-76dc-c85b-80ac-08da51bdaf71'
            request_body['sourceAddress'] = "rabbitmq://194.5.188.18:8443/AI01_TestRabbitMq_bus_7qayyyds5urfs8fkbdpfdxpp84?temporary=true"
            request_body['destinationAddress'] = f'rabbitmq://194.5.188.18:8443/{CORE_PUBLISH_QUEUE}'
            request_body["messageType"]: [f"urn:message:TestRabbitMq:{CORE_PUBLISH_QUEUE}"]
            response = sender.create_masstransit_response({'request_id':_id, 'data':{"images":SAMPLE_IMAGE}}, request_body)
        
            sender.publish(message=response)
            print(response)
            print('The message is sent to CORE_PUBLISH_QUEUE!')
    
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=RAPID_CONSUME_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    # create sender and send a value
    with DurableRabbitMQSender(sender_conf) as sender:
        sender.set_exchange(RAPID_CONSUME_QUEUE)
        
        for i in range(rep):
            _id = f'RAPID_CONSUME_QUEUE{i}'
        
            request_body = {}
            request_body['messageId'] = '0afe66e7-d89e-47e7-964e-153f7afda4b4'
            request_body['conversationId'] = 'ebb00000-76dc-c85b-80ac-08da51bdaf71'
            request_body['sourceAddress'] = "rabbitmq://194.5.188.18:8443/AI01_TestRabbitMq_bus_7qayyyds5urfs8fkbdpfdxpp84?temporary=true"
            request_body['destinationAddress'] = f'rabbitmq://194.5.188.18:8443/{RAPID_CONSUME_QUEUE}'
            request_body["messageType"]: [f"urn:message:TestRabbitMq:{RAPID_CONSUME_QUEUE}"]
            response = sender.create_masstransit_response({'request_id':_id, 'data':{"images":SAMPLE_IMAGE}}, request_body)
        
            sender.publish(message=response)
            print(response)
            print('The message is sent to RAPID_CONSUME_QUEUE!')
    
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=UTIL_CONSUME_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    # create sender and send a value
    with DurableRabbitMQSender(sender_conf) as sender:
        sender.set_exchange(UTIL_CONSUME_QUEUE)
        
        for i in range(rep):
            _id = f'UTIL_CONSUME_QUEUE{i}'
        
            request_body = {}
            request_body['messageId'] = '0afe66e7-d89e-47e7-964e-153f7afda4b4'
            request_body['conversationId'] = 'ebb00000-76dc-c85b-80ac-08da51bdaf71'
            request_body['sourceAddress'] = "rabbitmq://194.5.188.18:8443/AI01_TestRabbitMq_bus_7qayyyds5urfs8fkbdpfdxpp84?temporary=true"
            request_body['destinationAddress'] = f'rabbitmq://194.5.188.18:8443/{UTIL_CONSUME_QUEUE}'
            request_body["messageType"]: [f"urn:message:TestRabbitMq:{UTIL_CONSUME_QUEUE}"]
            response = sender.create_masstransit_response({'request_id':_id, 'data':{"images":SAMPLE_IMAGE}}, request_body)
        
            sender.publish(message=response)
            print(response)
            print('The message is sent to UTIL_CONSUME_QUEUE!')
