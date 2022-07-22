# from codes.processing_core import * 
import os
from masstransitpython import RabbitMQSender

import base64
from io import BytesIO
from PIL import Image
import tensorflow as tf
import tensorflow_hub as hub
import numpy as np

# 

from queue_wrapper import * 

MODEL_PATH = './model/'
threshold = 0.5
detector = hub.load(MODEL_PATH)

def detect_objects(path: str, model) -> dict:
    """Function extracts image from a file, adds new axis
    and passes the image through object detection model.
    :param path: File path
    :param model: Object detection model
    :return: Model output dictionary
    """
    image_tensor = tf.image.decode_jpeg(
        tf.io.read_file(path), channels=3)[tf.newaxis, ...]
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
    # create sender and send a value
    with DurableRabbitMQSender(sender_conf) as sender:
        sender.set_exchange(PUBLISH_QUEUE)
        string_image = body['message']['data']
        byte_image = bytes(string_image, 'utf-8')
        im_bytes = base64.b64decode(byte_image)
        im_file = BytesIO(im_bytes)  # convert image to file-like object
        img = Image.open(im_file)   # 
        img.save('./temp.jpg')
        results = detect_objects('./temp.jpg', detector)
        count = (results['detection_classes'].numpy()[0] == 1)[np.where(
        results['detection_scores'].numpy()[0] > threshold)].sum()

        
        # if True:
        #     print(f'{HANDLER_OPERATION} $ process is invoked')
        #     print('')
        _id = body['message']['id']
        response = sender.create_masstransit_response({'id':_id, 'Count':str(count)}, body)
        sender.publish(message=response)
        if True:
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
 