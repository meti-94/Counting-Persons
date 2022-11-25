# Crowd Counting

`Crowd Counting` is a `tool` which allows `users` to `count the number of people visible in an image`.
# Prerequisites
1. python >= 3.7
2. tensorflow==2.8.2
3. tensorflow-hub==0.12.0
4. pika==1.3.0
5. masstransitpython-byQ96==0.0.5


# Getting Start
* build image
* run image
* it will be binding to the exchange/queue specified in the config file
# Usage
build
```powershell
docker build -t crowdcounting .
```
run
```powershell
docker run -it --env-file .\DockerUtilities\env.conf  crowdcounting
```

**Note:** there is a script named `test.py` which can be used for testing functionality. Keep in mind it must run from another docker container. 

# Change log
Stable Version `v1.0.0`