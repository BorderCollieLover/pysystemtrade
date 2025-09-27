#!/bin/bash

cd /mnt/sda1/pysystemtrade
source .venv/bin/activate
python mttestscripts/test_seed_prices_from_ib.py&
python mttestscripts/add_PST_contract_key_to_futures_contract_db.py&
