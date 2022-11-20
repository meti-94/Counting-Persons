import requests
import uuid
import time
import threading
import logging



def p_process(q1, q2, q3, b):
    while True:
        if (q1.qsize()!=0 and q2.qsize()<=b.value):
            raw = q1.get()
            result = {}
            err = {}
            _id = list(raw.keys())[0]
            try:
                images = list(raw.values())[0]
                download_threads = []
                tic = time.time()
                for idx, img in enumerate(images):
                    temp_thread = threading.Thread(target=download_image, args=(idx, img, result, err))
                    temp_thread.start()
                    download_threads.append(temp_thread)
                for thread in download_threads:
                    thread.join()
            except Exception as e:
                error = {_id:(str(e)+' Pre-process Process', list(result.values()))}
                q3.put(error)
            else:
                if len(err):
                    error = {_id:(list(err.values())[0]+'Download Thread => Pre-process Process', list(result.values()))}
                    q3.put(error)
                else:
                    logging.warning(f'Download Is Done In : {(time.time()-tic)} seconds')
                    processed = {_id:[result[idx] for idx in range(len(images))]}
                    q2.put(processed)
        else:
            time.sleep(2)

def download_image(name, link, result, error):
    """Gets download link and saves the file with specified name in the current directory

    :param name: The name of downloaded image 
    :type name: :class:`str`
    
    :param link: The link to download the image 
    :type link: :class:`str`

    :param result: The resulted image files  
    :type result: :class:`dict`

    :param error: If any error has occured 
    :type error: :class:`dict`

    :raises Connection Timeout: Occures whenever the connection has been lost
    


    :return: None
    :rtype:  :class:`None`
    """
    try:
        remote_url = link
        local_file_name = f'{str(uuid.uuid4())}.jpg'
        data = requests.get(remote_url)
        with open(local_file_name, 'wb')as file:
            file.write(data.content)
        result[name]=local_file_name
        return True
    except Exception as e:
        logging.warning(f'Download Error')
        error[link] = str(e)