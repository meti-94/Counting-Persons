import cv2
import torch
from PIL import Image
from inspect import signature


threshold = 0.5

# Model
model = torch.hub.load('ultralytics/yolov5', 'yolov5s')

def detect(batch:list):
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


