#!/bin/bash

cd /mnt/sda1/pysystemtrade
source .venv/bin/activate
python mttestscripts/test_seed_prices_from_ib.py&
