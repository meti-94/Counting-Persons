from masstransitpython import RabbitMQSender
import logging
import time
import threading
import multiprocessing as mp

from codes.inference import *
from codes.preprocess import *
from codes.queue_wrapper import * 





if __name__ == "__main__":

    # Object to be shared between different Processes
    manager = mp.Manager()
    d = manager.dict()
    q1 = mp.Queue()
    q2 = mp.Queue()
    q3 = mp.Queue()
    b = mp.Value('i', 3)
    
    # Initialization and attaching while-true process for inference and preprocess
    PP = mp.Process(target=p_process, args=(q1, q2, q3, b))
    PP.start()
    IP = mp.Process(target=i_process, args=(q2, q3))
    IP.start()
    

    # defining credentials and configuration for receiver
    credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    receiver_conf = RabbitMQConfiguration(credentials,
                                 queue=CONSUME_QUEUE,
                                 host=RABBITMQ_HOST,
                                 port=RABBITMQ_PORT,
                                 virtual_host=RABBITMQ_VIRTUAL_HOST)

    receiver = DurableRabbitMQReceiver(receiver_conf, CONSUME_QUEUE)
    
    # defining credentials and configuration for sender
    sender_conf = RabbitMQConfiguration(credentials,
                                        queue=PUBLISH_QUEUE,
                                        host=RABBITMQ_HOST,
                                        port=RABBITMQ_PORT,
                                        virtual_host=RABBITMQ_VIRTUAL_HOST)
    sender = DurableRabbitMQSender(sender_conf)
    sender.set_exchange(PUBLISH_QUEUE)
    logging.warning('Connection Established!')
    
    # defining an ever-running loop in the main process
    while True:
        # checking if there any message to be sent!
        if q3.qsize()!=0: 
                infered = q3.get()
                _id = list(infered.keys())[0]
                counts = list(infered.values())[0]
                # check if any error has occurred
                if isinstance(counts[0], str):
                    response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":[]}, 'issuccessful':False, 'exception':counts[0]}, d[_id])
                    sender.publish(message=response)
                    for img in counts[1]:
                        os.remove(img)
                    del d[_id]
                # or if every thing is all right!
                else:
                    response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":counts}, 'issuccessful':True, 'exception':''}, d[_id])
                    sender.publish(message=response)
                    del d[_id]
        # if the buffer is free for preprocess another batch of input
        if q1.qsize()<=b.value:
            method_frame, header_frame, body = receiver._channel.basic_get(receiver._queue)
            if method_frame:
                true = 'True'
                _id = eval(body.decode('utf-8'))['message']['request_id']
                images = eval(body.decode('utf-8'))['message']['data']['images']
                
                data = {_id:images}
                dc = data.copy()
                q1.put(dc)
                data.clear()

                d[_id] = eval(body.decode('utf-8'))

                receiver._channel.basic_ack(method_frame.delivery_tag)         
        # none of the above scenarios happend!
        else:
            time.sleep(1)
            

