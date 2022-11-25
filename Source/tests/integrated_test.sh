#!/usr/bin/env bash
while read line; do echo $line; done < tests/test_setup.txt
source ./tests/test_setup.txt
python3 pipeline.py & 
python3 ./tests/test.py
pkill python