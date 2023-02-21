from masstransitpython import RabbitMQSender
import logging
import time
import sys
import threading
import multiprocessing as mp
import multiprocessing.managers as mpm
from queue import PriorityQueue

from codes.consumer import *
from codes.publisher import *
from codes.inference import *
from codes.preprocess import *

from codes.queue_wrapper import * 



logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.WARNING)



class MyManager(mpm.SyncManager):
    pass
MyManager.register("PriorityQueue", PriorityQueue)  # Register a shared PriorityQueue

def Manager():
    m = MyManager()
    m.start()
    return m



if __name__ == "__main__":
    
    threshold = mp.Value('i', 10)
    m = Manager()
    links_priority_queue = m.PriorityQueue()  # This is process-safe
    images_queue = mp.Queue()
    counts_queue = mp.Queue()
    

    consume_process = mp.Process(target = consume, args = (links_priority_queue, threshold))
    consume_process.start()
    # consume_process.join()
    

    preprocess_process = mp.Process(target = p_process, args = (links_priority_queue, images_queue))
    preprocess_process.start()
    # preprocess_process.join()
    

    inference_process = mp.Process(target = i_process, args = (images_queue, counts_queue))
    inference_process.start()
    # inference_process.join()

    publisher_process = mp.Process(target = publish, args = (counts_queue,))
    publisher_process.start()
    # publisher_process.join()

    while True:
        logging.warning(f'Size of Links queue:{links_priority_queue.qsize()}\tSize of Links queue:{images_queue.qsize()}\tSize of Links queue:{counts_queue.qsize()}')
        time.sleep(5)
        # print("In consume():Removed an element from the Queue");
    # try:
    #     # Object to be shared between different Processes
    #     manager = mp.Manager()
    #     d = manager.dict()
    #     q1 = mp.Queue()
    #     q2 = mp.Queue()
    #     q3 = mp.Queue()
    #     b = mp.Value('i', 3)
        
    #     # Initialization and attaching while-true process for inference and preprocess
    #     PP = mp.Process(target=p_process, args=(q1, q2, q3, b))
    #     PP.start()
    #     IP = mp.Process(target=i_process, args=(q2, q3))
    #     IP.start()
        

    #     # defining credentials and configuration for receiver
    #     credentials = PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    #     receiver_conf = RabbitMQConfiguration(credentials,
    #                                  queue=CONSUME_QUEUE,
    #                                  host=RABBITMQ_HOST,
    #                                  port=RABBITMQ_PORT,
    #                                  virtual_host=RABBITMQ_VIRTUAL_HOST)

    #     receiver = DurableRabbitMQReceiver(receiver_conf, CONSUME_QUEUE)
        
    #     # defining credentials and configuration for sender
    #     sender_conf = RabbitMQConfiguration(credentials,
    #                                         queue=PUBLISH_QUEUE,
    #                                         host=RABBITMQ_HOST,
    #                                         port=RABBITMQ_PORT,
    #                                         virtual_host=RABBITMQ_VIRTUAL_HOST)
    #     sender = DurableRabbitMQSender(sender_conf)
    #     sender.set_exchange(PUBLISH_QUEUE)
    #     logging.warning('Connection Established!')
        
    #     # defining an ever-running loop in the main process
    #     while True:
    #         # checking if there any message to be sent!
    #         if q3.qsize()!=0: 
    #                 infered = q3.get()
    #                 _id = list(infered.keys())[0]
    #                 counts = list(infered.values())[0]
    #                 # check if any error has occurred
    #                 if isinstance(counts[0], Exception):

    #                     e = counts[0]
    #                     response = sender.create_masstransit_response({'response_id':_id, 
    #                                                                    'data':{"counts":[]}, 
    #                                                                    'issuccessful':False, 
    #                                                                    'exception':f"{type(e).__name__}: {e}",
    #                                                                    'exception_code':408},
    #                                                                    d[_id])
    #                     sender.publish(message=response)
    #                     logging.warning(response)
    #                     if len(counts)==2 and isinstance(counts[1], list):
    #                         for img in counts[1]:
    #                             os.remove(img)
    #                     del d[_id]
    #                 # or if every thing is all right!
    #                 else:
    #                     response = sender.create_masstransit_response({'response_id':_id, 'data':{"counts":counts}, 'issuccessful':True, 'exception':''}, d[_id])
    #                     sender.publish(message=response)
    #                     logging.warning(response)
    #                     del d[_id]
    #         # if the buffer is free for preprocess another batch of input
    #         if q1.qsize()<=b.value:
    #             try:
    #                 method_frame, header_frame, body = receiver._channel.basic_get(receiver._queue)
    #             except:
    #                 continue
    #             if method_frame:
    #                 true = 'True'
    #                 try:
    #                     _id = eval(body.decode('utf-8'))['message']['request_id']
    #                 except Exception as e:
    #                     if str(e)=="'request_id'":
    #                         _id = str(uuid.uuid4())
    #                         temp = {_id:eval(body.decode('utf-8'))}
    #                         response = sender.create_masstransit_response({'response_id':_id, 
    #                                                                         'data':{"counts":[]}, 
    #                                                                         'issuccessful':False, 
    #                                                                         'exception':f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}",
    #                                                                         'exception_code':400},
    #                                                                        temp[_id])
    #                         sender.publish(message=response)
    #                         logging.warning(response)
    #                         receiver._channel.basic_ack(method_frame.delivery_tag)
    #                         continue        
    #                 try:
    #                     images = eval(body.decode('utf-8'))['message']['data']['images']
    #                 except Exception as e:
    #                     if str(e)=="'data'":   
    #                         temp = {_id:eval(body.decode('utf-8'))}
    #                         response = sender.create_masstransit_response({'response_id':_id, 
    #                                                             'data':{"counts":[]}, 
    #                                                             'issuccessful':False, 
    #                                                             'exception':f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}",
    #                                                             'exception_code':204},
    #                                                            temp[_id])
    #                         sender.publish(message=response)
    #                         logging.warning(response)
    #                         receiver._channel.basic_ack(method_frame.delivery_tag)  
    #                         continue

    #                 data = {_id:images}
    #                 dc = data.copy()
    #                 q1.put(dc)
    #                 data.clear()

    #                 d[_id] = eval(body.decode('utf-8'))

    #                 receiver._channel.basic_ack(method_frame.delivery_tag)         
    #         # none of the above scenarios happend!
    #         else:
    #             time.sleep(1)
    # except Exception as e:
    #     print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    #     PP.terminate()
    #     IP.terminate()
    #     sys.exit(1)
                

