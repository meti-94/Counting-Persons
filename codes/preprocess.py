import requests

def download_image(name, link):
    """Gets download link and saves the file with specified name in the current directory

    :param name: The name of downloaded image 
    :type name: :class:`str`
    
    :param link: The link to download the image 
    :type link: :class:`str`

    :raises Connection Timeout: Occures whenever the connection has been lost
    


    :return: A list which would be filled with strings representing the number of persions in each image
    :rtype:  :class:`List[str]`
    """
    remote_url = link
    local_file_name = f'{name}.jpg'
    data = requests.get(remote_url)
    with open(local_file_name, 'wb')as file:
        file.write(data.content)
    return True