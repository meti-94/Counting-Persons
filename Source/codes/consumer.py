from codes.queue_wrapper import * 
from masstransitpython import RabbitMQSender
import time
import os 
import logging

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_PORT = os.environ['RABBITMQ_PORT']
RABBITMQ_VIRTUAL_HOST = os.environ['RABBITMQ_VIRTUAL_HOST']

CORE_CONSUME_QUEUE = os.environ['CORE_CONSUME_QUEUE']
RAPID_CONSUME_QUEUE = os.environ['RAPID_CONSUME_QUEUE']
UTIL_CONSUME_QUEUE = os.environ['UTIL_CONSUME_QUEUE']


def consume(links_priority_queue, threshold):
    """
    Fills the corresponding queue with the appropriate response of :class:`dict[str]` representing
    the number of persons in each image inside a batch. In case of any error occurred it fills 
    exception key of the dictionary.

    :param links_priority_queue: A Priority Queue contains input batches of image paths filled with the consumer process. 
    :type links_priority_queue: :class:`priority queue`    
    :param threshold: An integer value indicates the number of buffered requests. 
    :type dataframe: :class:`int`

    :raises Connection Error: Occurs whenever the program can't find Rabbit
    
    """
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    core_receiver_conf = RabbitMQConfiguration(credentials,
                                 queue=CORE_CONSUME_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)
    core_receiver = DurableRabbitMQReceiver(core_receiver_conf, CORE_CONSUME_QUEUE)
    
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    rapid_receiver_conf = RabbitMQConfiguration(credentials,
                                 queue=RAPID_CONSUME_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)
    rapid_receiver = DurableRabbitMQReceiver(rapid_receiver_conf, RAPID_CONSUME_QUEUE)
    
    util_receiver_conf = RabbitMQConfiguration(credentials,
                                 queue=UTIL_CONSUME_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)
    util_receiver = DurableRabbitMQReceiver(util_receiver_conf, UTIL_CONSUME_QUEUE)
    logging.warning('Consumer Connection Established!')
    while True:
        if links_priority_queue.qsize()<=threshold.value:
            try:
                method_frame, header_frame, body = util_receiver._channel.basic_get(util_receiver._queue)
            except Exception as e:
                logging.info('Checking RBTMQ is faced an issue.')
                logging.info(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                time.sleep(1)
                continue
            # print('util_receiver', method_frame)
            if method_frame:
                item = parser(body)
                item = (1, id(item), item)
                links_priority_queue.put(item)
                util_receiver._channel.basic_ack(method_frame.delivery_tag)
            try:
                method_frame, header_frame, body = rapid_receiver._channel.basic_get(rapid_receiver._queue)
            except Exception as e:
                logging.info('Checking RBTMQ is faced an issue.')
                logging.info(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                time.sleep(1)
                continue
            # print('rapid_receiver', method_frame)
            if method_frame:
                item = parser(body)
                item = (2, id(item), item)
                links_priority_queue.put(item)
                rapid_receiver._channel.basic_ack(method_frame.delivery_tag)
            try:
                method_frame, header_frame, body = core_receiver._channel.basic_get(core_receiver._queue)
            except Exception as e:
                logging.info('Checking RBTMQ is faced an issue.')
                logging.info(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                time.sleep(1)
                continue
            # print('core_receiver', method_frame)
            if method_frame:
                item = parser(body)
                item = (3, id(item), item)
                links_priority_queue.put(item)
                core_receiver._channel.basic_ack(method_frame.delivery_tag)
        else:
            time.sleep(1)
            


def parser(message_body):

    """
    It Parses the body of rabbitMQ message into a predefined template. 
    In case of failure, it includes the first exception in using its key. 
    
    :param message_body: A list filled with the the paths related to each image. 
    :type message_body: :class:`dict`

    :raises Key not found: Occurs whenever the program can parse the input

    :return: A dict that would be filled with the appropriate set of data from the message body
    :rtype:  :class:`dict` 

    """

    payload = {}
    try:
        true = 'True'
        _id = eval(message_body.decode('utf-8'))['message']['request_id']
        payload['id'] = _id
        images = eval(message_body.decode('utf-8'))['message']['data']['images']
        payload['links'] = images
        payload['exception'] = ''
        payload['exception_code'] = ''
        payload['issuccessful'] = True
        payload['messageId'] = eval(message_body.decode('utf-8'))['messageId']
        payload['conversationId'] = eval(message_body.decode('utf-8'))['conversationId']
        payload['sourceAddress'] = eval(message_body.decode('utf-8'))['sourceAddress']
        payload['destinationAddress'] = eval(message_body.decode('utf-8'))['destinationAddress']
    except Exception as e:
        payload['links'] = []
        payload['exception'] = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        logging.warning( f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        payload['exception_code'] = 204
        payload['issuccessful'] = False
        payload['messageId'] = eval(message_body.decode('utf-8'))['messageId']
        payload['conversationId'] = eval(message_body.decode('utf-8'))['conversationId']
        payload['sourceAddress'] = eval(message_body.decode('utf-8'))['sourceAddress']
        payload['destinationAddress'] = eval(message_body.decode('utf-8'))['destinationAddress']
    return payload