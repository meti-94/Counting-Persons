from masstransitpython import RabbitMQSender
import logging
import time
import threading

from codes.inference import *
from codes.preprocess import *
from codes.queue_wrapper import * 




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
        
        ############# Preprocess for a request, For IO bounds multi-thread for CPU bounds multi-process
        download_threads = []
        tic = time.time()
        for idx, img in enumerate(images):
            temp_thread = threading.Thread(target=download_image, args=[idx, img])
            temp_thread.start()
            download_threads.append(temp_thread)
        for thread in download_threads:
            thread.join()
        ############# Elapsed time
        logging.warning(f'Download Is Done In : {(time.time()-tic)} seconds')
        ############# Inference
        batch = [f'./{idx}.jpg' for idx, img in enumerate(images)]
        counts = detect(batch)
        ############# Elapsed time
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
 