#!/usr/bin/env bash
python3 pipeline.py & 
python3 ../test/test.py
pkill python
