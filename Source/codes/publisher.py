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

def publish(counts_queue):
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    sender_conf = RabbitMQConfiguration(credentials,
                                 queue=PUBLISH_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)
    sender = DurableRabbitMQSender(sender_conf)
    sender.set_exchange(PUBLISH_QUEUE)
    logging.warning('Publisher Connection Established!')
    while True:
        if counts_queue.empty() == False:
            item_dict = counts_queue.get()
            logging.warning(item_dict)
            request_body = {
                'messageId':item_dict['messageId'],
                'conversationId':item_dict['conversationId'],
                'sourceAddress':item_dict['sourceAddress'],
                'destinationAddress':item_dict['destinationAddress']
            }
            response = sender.create_masstransit_response({'response_id':item_dict['id'], 
                                                           'data':{"counts":item_dict['counts']}, 
                                                           'issuccessful':item_dict['issuccessful'], 
                                                           'exception':item_dict['exception'],
                                                           'exception_code':item_dict['exception_code']},
                                                           request_body)
            sender.publish(message=response)
            logging.warning(response)
        else:
            time.sleep(2)