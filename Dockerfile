FROM ubuntu:latest
RUN apt update && \
 apt install -y vim && \
 apt-get install -y python3-pip 
RUN pip3 install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu
COPY ./Source/requirements.txt /req.txt
RUN pip3 install -r /req.txt
RUN python3 -c "import torch; model = torch.hub.load('ultralytics/yolov5', 'yolov5s')"
COPY ./Source/*.* /source/
COPY ./Source/codes /source/codes
COPY ./Test/* /test/
WORKDIR /source
CMD [ "python3", "pipeline.py" ]
