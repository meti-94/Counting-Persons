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
                logging.warning("Error: %s : %s" % (dir_path, e.strerror))     
        else:
            time.sleep(2)


# def i_process(q2, q3):
#     model = torch.hub.load('ultralytics/yolov5', 'yolov5s')
#     while True:
#         if q2.qsize()!=0:
#             processed = q2.get()
#             _id = list(processed.keys())[0]
#             try:
#                 images = list(processed.values())[0]
#                 tic = time.time()
#                 counts = detect(images, model)
#                 logging.warning(f'{_id} Inference Is Done In : {(time.time()-tic)} seconds')
#                 infered = {_id:counts}
#                 q3.put(infered)
#                 for img in images:
#                     os.remove(img)
#             except Exception as e:
#                 error = {_id:(e, )}
#                 q3.put(error)
#         else:
#             time.sleep(2)

def detect(batch:list, model) -> list:
    """Returns an instance of :class:`List[str]` representing
    the the number of persons each image inside a batch. 

    :param dataframe: A list filled with the the paths related to each image. 
    :type dataframe: :class:`List[str]`
    
    :raises Model not found: Occures whenever the program can't find model file
    :raises Image not found: Occures whenever the program can't find input image
    


    :return: A list which would be filled with strings representing the number of persions in each image
    :rtype:  :class:`List[str]`
    """
    images = [Image.open(item) for item in batch]
    results = model(images, size=320)
    extract = lambda item: item[(item['name']=='person') & (item['confidence']>= threshold)].shape[0]
    counts = [extract(item) for item in results.pandas().xyxy]
    return counts
