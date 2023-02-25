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
    """
    Creating a manager capable of passing more advanced data structures between processes
    """

    m = MyManager()
    m.start()
    return m



if __name__ == "__main__":
    """
    1. Creating required data structures to be shared between processes
    2. Initiating appropriate process with the proper argument set
    3. Starting processes and creating a forever loop. 
    """
    
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
