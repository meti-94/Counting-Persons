
# Crowd Counting Module

`Crowd Counting` is a `microservice` that allows `users` to `the count number of people in a scene`.
# Prerequisites
1. windows/linux 
2. pytorch
3. pika

**Attention**: The service is using plug and play model which would be downloaded and cached after calling `model = torch.hub.load('ultralytics/yolov5', 'yolov5s')` 
# Getting Start
* Install necessary libreries 
* Set up remote/local RabbitMQ
* Set environment variables for Queue credentials
* Run `python pipeline.py`
* Run the `Source/tests/test.py`
* There is a shell script containing best practices for starting in `Source/Test/integrated_test.sh`
# Usage
build
```powershell
docker build -t crowdcounting:version
```
run
```powershell
docker run -it --env-file .\DockerUtilities\conv.env crowdcounting
```

# Change log
* `1.X`: prototyping
* `2.X`: Error handling
* `3.X`: Stable Version 

