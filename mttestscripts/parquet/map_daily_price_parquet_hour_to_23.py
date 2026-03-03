import os
import re
from enum import Enum
import pandas as pd
from sysdata.data_blob import dataBlob
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.parquet.parquet_access import EXTENSION as PARQUET_EXTENSION
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData, CONTRACT_COLLECTION
from sysproduction.data.broker import dataBroker
from sysproduction.update_historical_prices import write_merged_prices_for_contract
from sysproduction.data.prices import diagPrices
from sysobjects.contracts import futuresContract
from sysobjects.futures_per_contract_prices import futuresContractPrices
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, HOURLY_FREQ, month_from_contract_letter, contract_month_from_number
from mtfuturesdata.mtMongoClient import mtMongoClient
from mttestscripts.parquet.ohlc_parquet_cleanup_tools import clean_up_ohlc

instrument_code = 'NICKEL_LME'
contract_dt = '20240100'
contract=futuresContract(instrument_code, contract_dt)

parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
parquet_price = parquetFuturesContractPriceData(parquet_access)
data = dataBlob(log_name="update_historical_prices")

if not parquet_price.has_price_data_for_contract_at_frequency(contract, DAILY_PRICE_FREQ):
    print(f"No parquet price data for {contract} at daily frequency")

pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, DAILY_PRICE_FREQ)
pst_prices.index = pst_prices.index.map(lambda x: x.replace(hour=23, minute=0))
parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, pst_prices, DAILY_PRICE_FREQ)
print('writing merged prices.....')
write_merged_prices_for_contract(data, contract, [HOURLY_FREQ, DAILY_PRICE_FREQ])

print(pst_prices)