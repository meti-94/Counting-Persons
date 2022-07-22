FROM tensorflow/tensorflow:2.8.2
RUN apt update && \
 apt install -y vim && \
 apt-get install -y python3-pip 
COPY ./requirements.txt /req.txt
RUN pip3 install -r /req.txt
COPY ./pipeline.py /pipeline.py
COPY ./queue_wrapper.py /queue_wrapper.py
COPY ./ourown_test.py /test.py
COPY ./test1.jpg /test1.jpg

CMD [ "python3", "/pipeline.py" ]