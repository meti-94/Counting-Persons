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

# This script is aiming at mimic the works are being done in case of raw pushing data into the rabbitmq

# credentials in plain text is placed here intentionally

# RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
# RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
# RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
# RABBITMQ_PORT = os.environ['RABBITMQ_PORT']
# RABBITMQ_VIRTUAL_HOST = os.environ['RABBITMQ_VIRTUAL_HOST']

# PUBLISH_QUEUE = os.environ['CONSUME_QUEUE']
# CONSUME_QUEUE = os.environ['PUBLISH_QUEUE']


from queue_wrapper import *

global count
global rep

SAMPLE_IMAGE = ["https://iili.io/myctrN.jpg"]*25
# SAMPLE_IMAGE = ["https://i.im.ge/2022/07/28/F9kOzG.jg"]*2
# SAMPLE_IMAGE = ["https://amnazmoon.com/AppData/tehranExamsUNI/RandomShot/Image/9126302730/53672884-018e-42ff-9695-8e2758383a9c.jpg"]
SAMPLE_IMAGE = ["https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/dad65df0-020e-4250-92a0-0f0056afb663.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/05952432-a2e0-4f56-b93c-2b3a5ed3912d.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/5200ad88-d153-4c1b-ad7a-26a450b26d74.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/ff061e64-f96f-41cb-9a4c-3325a6ea7f0b.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/150ef213-d05d-464f-8782-41f79564ff07.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/1e80c995-8a2a-4e7c-b49e-546f2cc7d288.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/d1869255-5cff-48fe-af4a-6e50370229b9.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/3b9fe11c-4a66-4e5e-9b92-7278efc9c961.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/b06cd693-e7aa-486a-a518-8c3baa026448.jpg",
                "https://amnazmoon.faran.ac.ir/AppData/Tehran-ExamUNI/RandomShot/Image/1087/326d389b-5406-4f0d-9098-8c58593935ce.jpg"]
                
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
    global count
    global rep
    count+=1
    null = None
    print(msg['message'])
    print('')


if __name__ == "__main__":
    global count
    count = 0
    rep = 2
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=PUBLISH_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    # create sender and send a value
    with DurableRabbitMQSender(sender_conf) as sender:
        sender.set_exchange(PUBLISH_QUEUE)
        
        for i in range(rep):
            _id = str(uuid.uuid4())
        
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
        credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
        conf = RabbitMQConfiguration(credentials,
                                 queue=CONSUME_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)
    
        # define receiver
        receiver = DurableRabbitMQReceiver(conf, CONSUME_QUEUE)
        receiver.add_on_message_callback(handler)
        tic = time()
        receiver.start_consuming()
