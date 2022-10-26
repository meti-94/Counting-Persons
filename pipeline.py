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
import logging
import time
import threading
import concurrent.futures
from tqdm import tqdm

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
    

def multiprocess_detect(path: str) -> tuple:
    model = hub.load(MODEL_PATH)
    image_tensor = tf.image.decode_jpeg(
        tf.io.read_file(path), channels=3)[tf.newaxis, ...]
    result = model(image_tensor)
    count = (result['detection_classes'].numpy()[0] == 1)[np.where(result['detection_scores'].numpy()[0] > threshold)].sum()
    return path.split('.')[0], str(count)

def mp_detect_objects(path: str, model) -> dict:
    """Function extracts image from a file, adds new axis
    and passes the image through object detection model.
    :param path: File path
    :param model: Object detection model
    :return: Model output dictionary
    """
    image_tensor = tf.image.decode_jpeg(
        tf.io.read_file(path), channels=3)[tf.newaxis, ...]
    return eval(path[2:-4]), model(image_tensor)

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
    if VERBOSE: 
        logging.warning('What is read from RBMQ:')
        logging.warning(list(msg['message'].keys()))
        logging.warning(list(msg['message'].values()))
        logging.warning('')
    t = Thread(target=send_message(msg))
    t.start()
    threads.append(t)

def send_message(body):
    _id = 'NULL'
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
        download_threads = []
        tic = time.time()
        for idx, img in enumerate(images):
            # res = download_image(idx, img)
            temp_thread = threading.Thread(target=download_image, args=[idx, img])
            temp_thread.start()
            download_threads.append(temp_thread)
        for thread in download_threads:
            thread.join()
        
        logging.warning(f'Download Is Done In : {(time.time()-tic)} seconds')
        counts = []
        processes = []
        indices = []
        tic = time.time()

        # for idx, img in enumerate(images):
        #     results = detect_objects(f'./{idx}.jpg', detector)
        #     count = (results['detection_classes'].numpy()[0] == 1)[np.where(results['detection_scores'].numpy()[0] > threshold)].sum()
        #     counts.append(str(count))
        
        ###########################################
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(mp_detect_objects, f'./{idx}.jpg', detector) for idx, img in enumerate(images)]
            
            for f in tqdm(concurrent.futures.as_completed(results)):
                processes.append(f.result())
        dictionary_result = dict()
        for idx, item in processes:
            count = (item['detection_classes'].numpy()[0] == 1)[np.where(item['detection_scores'].numpy()[0] > threshold)].sum()
            dictionary_result[idx] = str(count)
        
        dictionary_result = {k: v for k, v in sorted(list(dictionary_result.items()))}
        counts = list(dictionary_result.values())
        ###########################################

        logging.warning(f'Inference Is Done In : {(time.time()-tic)} seconds')
       
        response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":counts}, 'issuccessful':True, 'exception':''}, body)
        sender.publish(message=response)
    except Exception as e:
        response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":[]}, 'issuccessful':False, 'exception':str(e)}, body)
        sender.publish(message=response)
    if True:
        logging.warning('')
        logging.warning(response)
        logging.warning('The message is sent!')
        logging.warning('')


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

    logging.warning('Connection Established!')
    # define receiver
    receiver = DurableRabbitMQReceiver(conf, CONSUME_QUEUE)
    receiver.add_on_message_callback(handler)
    receiver.start_consuming()
 