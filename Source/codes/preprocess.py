import requests
import uuid
import time
import threading
import logging
import os


def p_process(links_priority_queue, images_queue):
    """
    Fills the corresponding queue with the appropriate response of :class:`dict[str]` representing
    the number of persons in each image inside a batch. In case of any error occurred it fills 
    exception key of the dictionary.

    :param links_priority_queue: A Priority Queue contains input batches of image paths filled with the consumer process. 
    :type links_priority_queue: :class:`priority queue`    
    :param images_queue: A queue contains path of images after download (or any other kind of preprocessing). 
    :type images_queue: :class:`queue`   

    :raises Download error: Occurs whenever the image can't be downloaded
    
    """
    while True:
        if links_priority_queue.empty() == False:
            item = links_priority_queue.get()
            item_dict = item[-1]
            if item_dict['exception']!='':
                item_dict.pop('links')
                item_dict['images'] = []
                images_queue.put(item_dict)
            else:
                try:                    
                    _id, image_files = multithread_download(item_dict['id'], item_dict['links'])
                    item_dict.pop('links')
                    item_dict['images'] = image_files
                    assert _id == item_dict['id']
                    images_queue.put(item_dict)
                except Exception as e:
                    item_dict.pop('links')
                    item_dict['images'] = []
                    item_dict['exception'] = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}",
                    item_dict['exception_code'] = 204
                    item_dict['issuccessful'] = False
                    images_queue.put(item_dict)
            
        else:
            time.sleep(1)

def multithread_download(_id, links):
    """ 
    Downloads a list of links and stores them under a subdirectory of "_id" 
    in the tmp folder 

    :param _id: request identifier corresponds to each request. 
    :type _id: :class:`str`
    :param links: download links for each image in the batch. 
    :type links: :class:`List[str]`
    

    :raises Download error: Occurs whenever the image can't be downloaded


    :return: A list that would be filled with the paths for each image on local machine
    :rtype:  :class:`List[str]`
    """
    try:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmp', _id)
        os.mkdir(path)
        download_threads = []
        for idx, lnk in enumerate(links):
            temp_thread = threading.Thread(target=download, args=(idx, lnk, path))
            temp_thread.start()
            download_threads.append(temp_thread)
        for thread in download_threads:
            thread.join()
        image_files = [item for item in os.listdir(path) if os.path.isfile(os.path.join(path, item))] 
        image_files = [os.path.join(path, item) for item in image_files] 
        return _id, image_files
    except Exception as e:
        logging.warning(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        raise e

def download(name, link, path):
    try:
        remote_url = link
        file_name = os.path.join(path, f'{str(uuid.uuid4())}.jpg')
        # file_name = f'{path}/{str(uuid.uuid4())}.jpg'
        if is_url_image(remote_url):
            data = requests.get(remote_url)
            with open(file_name, 'wb')as file:
                file.write(data.content)
    except Exception as e:
        logging.warning(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        raise e


def is_url_image(image_url):
    """
    Checks if the passed URL is a valid image URL
    """
    image_formats = ("image/png", "image/jpeg", "image/jpg")
    r = requests.head(image_url)
    if r.headers["content-type"] in image_formats:
        return True
    return False

