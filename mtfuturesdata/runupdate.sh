#!/bin/bash

cd /mnt/sda1/pysystemtrade
source .venv/bin/activate
python mtfuturesdata/multiprocessingtest.py&
python mttestscripts/backup_non_pst_data_as_csvs.py&
python mtfuturesdata/mtIBFuturesContracts.py&
sleep 100
#python mtfuturesdata/1day.py&
sleep 100
#python mtfuturesdata/1hour.py& 
sleep 100
#python mtfuturesdata/15min.py& 
sleep 100
#python mtfuturesdata/5min.py&
