#!/bin/bash

cd /mnt/sda1/pysystemtrade
source venv/3.10.13/bin/activate
python mtfuturesdata/multiprocessingtest.py&
sleep 100
python mtfuturesdata/1day.py&
sleep 100
python mtfuturesdata/1hour.py& 
sleep 100
python mtfuturesdata/15min.py& 
sleep 100
python mtfuturesdata/5min.py&
