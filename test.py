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
import os
from PIL import Image
import base64
from io import BytesIO


# This script is aiming at mimic the works are being done in case of raw pushing data into the rabbitmq

# credentials in plain text is placed here intentionally

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_PORT = os.environ['RABBITMQ_PORT']
RABBITMQ_VIRTUAL_HOST = os.environ['RABBITMQ_VIRTUAL_HOST']

PUBLISH_QUEUE = os.environ['PUBLISH_QUEUE']
CONSUME_QUEUE = os.environ['CONSUME_QUEUE']

from queue_wrapper import *




SAMPLE_IMAGE = ['https://i.im.ge/2022/07/28/F9kOzG.jpg', 'https://amnazmoon.com/newtemplate/assets/img/3.jpg']
SAMPLE_IMAGE = ["https://iili.io/DB2sVV.jpg","https://iili.io/DB2sVV.jpg"]*10
SAMPLE_IMAGE+=['https://iili.io/D86J3b.jpg']
SAMPLE_IMAGE = ['https://iili.io/D86J3b.jpg']+SAMPLE_IMAGE
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


def handler(ch, method, properties, body):
    msg = loads(body.decode())

    null = None
    print(msg['message']['data'], type(msg['message']['data']))
    print(msg['message']['data']['counts'])

    print('')


if __name__ == "__main__":

    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=PUBLISH_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    # create sender and send a value
    with DurableRabbitMQSender(sender_conf) as sender:
        sender.set_exchange(PUBLISH_QUEUE)
        _id = '0afe66e7-d89e-47e7-964e-153f7afda4b4'
        
        request_body = {}
        request_body['messageId'] = '0afe66e7-d89e-47e7-964e-153f7afda4b4'
        request_body['conversationId'] = 'ebb00000-76dc-c85b-80ac-08da51bdaf71'
        request_body['sourceAddress'] = "rabbitmq://194.5.188.18:8443/AI01_TestRabbitMq_bus_7qayyyds5urfs8fkbdpfdxpp84?temporary=true"
        request_body['destinationAddress'] = f'rabbitmq://194.5.188.18:8443/{PUBLISH_QUEUE}'
        request_body["messageType"]: [f"urn:message:TestRabbitMq:{PUBLISH_QUEUE}"]
        response = sender.create_masstransit_response({'request_id':_id, 'data':{"images":SAMPLE_IMAGE}}, request_body)
        sender.publish(message=response)
        print(response)
        print('The message is sent!')


    #     ## getting the result

        credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
        conf = RabbitMQConfiguration(credentials,
                                 queue=CONSUME_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)
    
        # define receiver
        receiver = DurableRabbitMQReceiver(conf, CONSUME_QUEUE)
        receiver.add_on_message_callback(handler)
        receiver.start_consuming()
