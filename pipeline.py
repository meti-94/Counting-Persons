# from codes.processing_core import * 
import os
from masstransitpython import RabbitMQSender
import sys
import base64
from io import BytesIO
from PIL import Image
import tensorflow as tf
import tensorflow_hub as hub
import numpy as np
import requests

# 

from queue_wrapper import * 

MODEL_PATH = './model/'
threshold = 0.5
detector = hub.load(MODEL_PATH)


def download_image(name, link):

    remote_url = link
    local_file_name = f'{name}.jpg'
    data = requests.get(remote_url)
    with open(local_file_name, 'wb')as file:
        file.write(data.content)
    return True
    


def detect_objects(path: str, model) -> dict:
    """Function extracts image from a file, adds new axis
    and passes the image through object detection model.
    :param path: File path
    :param model: Object detection model
    :return: Model output dictionary
    """
    image_tensor = tf.image.decode_jpeg(
        tf.io.read_file(path), channels=3)[tf.newaxis, ...]
    # print(type(image_tensor))
    return model(image_tensor)


def handler(ch, method, properties, body):
    msg = loads(body.decode())
    if True: 
        print('What is read from RBMQ:')
        print(list(msg['message'].keys()))
        print('')
    t = Thread(target=send_message(msg))
    t.start()
    threads.append(t)

def send_message(body):
    # configure publisher
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=PUBLISH_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    try:
        _id = body['message']['request_id']
        res_array = []
        # create sender and send a value
        sender = DurableRabbitMQSender(sender_conf)
        
        sender.set_exchange(PUBLISH_QUEUE)
        images = body['message']['data']['images']
        for idx, img in enumerate(images):
            res = download_image(idx, img)

        print('Downloading is done!')
        counts = []
        for idx, img in enumerate(images):
            results = detect_objects(f'./{idx}.jpg', detector)
            count = (results['detection_classes'].numpy()[0] == 1)[np.where(results['detection_scores'].numpy()[0] > threshold)].sum()
            counts.append(str(count))
        
       
        response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":counts}, 'issuccessful':'true', 'exception':''}, body)
        sender.publish(message=response)
    except Exception as e:
        response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":[]}, 'issuccessful':'false', 'exception':str(e)}, body)
        sender.publish(message=response)
    if True:
        print(response)
        print('The message is sent!')
        print('')


if __name__ == "__main__":
    # define thread container
    threads = []
    # define credentials and configuration for receiver
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
 