import cv2
import torch
from PIL import Image
from inspect import signature
import time
import logging
import os
import shutil
from .queue_wrapper import *



threshold = 0.5
model = None

def i_process(images_queue, counts_queue):

    """
    Fills the corresponding queue with the appropriate response of :class:`dict[str]` representing
    the number of persons in each image inside a batch. In case of any error occurred it fills 
    exception key of the dictionary.

    :param images_queue: A Queue contains input batches of image paths filled with the consumer process. 
    :type dataframe: :class:`queue`    
    :param counts_queue: A Queue to be filled with the counts of people in the images. 
    :type dataframe: :class:`queue`

    :raises Model not found: Occurs whenever the program can't find model file
    :raises Image not found: Occurs whenever the program can't find input image
    

    """

    model = torch.hub.load('ultralytics/yolov5', 'yolov5s')
    while True:
        if images_queue.empty() == False:
            item_dict = images_queue.get()
            if item_dict['exception']!='':
                item_dict.pop('images')
                item_dict['counts'] = []
                counts_queue.put(item_dict)
            else:
                try:
                    counts = detect(item_dict['images'], model)                    
                    item_dict.pop('images')
                    item_dict['counts'] = counts
                    counts_queue.put(item_dict)
                except Exception as e:
                    item_dict.pop('images')
                    item_dict['counts'] = []
                    item_dict['exception'] = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}",
                    item_dict['exception_code'] = 204
                    item_dict['issuccessful'] = False
                    counts_queue.put(item_dict)   
            try:
                shutil.rmtree(os.path.join('./codes/tmp/', item_dict['id']))
            except OSError as e:
                logging.warning("Error: %s : %s" % (f'./codes/tmp/{item_dict["id"]}', e.strerror))     
        else:
            time.sleep(2)



def detect(batch:list, model) -> list:
    """ Returns an instance of :class:`List[str]` representing
    the number of persons in each image inside a batch. 

    :param dataframe: A list filled with the the paths related to each image. 
    :type dataframe: :class:`List[str]`
    
    :raises Memory Error: Occurs whenever the program can not fit into memory 
    


    :return: A list that would be filled with strings representing the number of persons in each image
    :rtype:  :class:`List[str]`
    """
    images = [Image.open(item) for item in batch]
    results = model(images, size=320)
    extract = lambda item: item[(item['name']=='person') & (item['confidence']>= threshold)].shape[0]
    counts = [extract(item) for item in results.pandas().xyxy]
    return counts
