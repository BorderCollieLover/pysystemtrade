#compare the contracts from seed_price_data_from_ib to my own database and see if there are any significant pockets of data I am missing
#The number of futures contract data files from seed_price_data_from_ib is close to the number of files I have been collecting separately
#This seems a bit odd as seed_price_data_from_ib collects data from 500+ instruments while my separate script collects all available contracts from IB

#not working yet 

from syscore.exceptions import missingData
from sysbrokers.IB.ib_futures_contract_price_data import (
    futuresContract,
)
from syscore.dateutils import DAILY_PRICE_FREQ, HOURLY_FREQ, Frequency
from sysdata.data_blob import dataBlob
from sysdata.config.production_config import get_production_config, Config
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION

from sysproduction.data.broker import dataBroker
from sysproduction.data.prices import updatePrices
from sysproduction.update_historical_prices import write_merged_prices_for_contract
import pandas as pd
from glob import glob
import datetime
from mtfuturesdata.count_parquet_data import count_one_parquet_file
from sysobjects.instruments import futuresInstrument
from sysobjects.contract_dates_and_expiries import contractDate
from syscore.constants import arg_not_supplied



def find_all_no_empty_parquet_files(file_path):
    all_files = glob(file_path+"/*.parquet")
    all_lines = [count_one_parquet_file(file) for file in all_files]
    no_empty_files = [file for file, line_count in zip(all_files, all_lines) if line_count > 0]
    return no_empty_files


def get_dataBroker():
    data= dataBlob()
    data_broker= dataBroker(data)
    return(data_broker)

def contract_list_from_instrument(instrument_code):
    data = dataBlob()
    data_broker = dataBroker(data)

    meta_data = data_broker.get_brokers_instrument_with_metadata(instrument_code=instrument_code)

    list_of_contracts = data_broker.get_list_of_contract_dates_for_instrument_code(
        instrument_code, allow_expired=True
    )
    print(list_of_contracts)

    ## This returns yyyymmdd strings, where we have the actual expiry date
    contracts = []
    for contract_date in list_of_contracts:
        date_str = contract_date
        contract_object = futuresContract(futuresInstrument(instrument_code), contractDate(date_str),arg_not_supplied, True)
        print(contract_object)
        contracts = contracts + [contract_object]

    return contracts

instrument_code= 'SP500'
data = dataBlob()
data_broker = dataBroker(data)
meta_data = data_broker.get_brokers_instrument_with_metadata(instrument_code=instrument_code)
print(meta_data)
contracts = contract_list_from_instrument(instrument_code)
data_broker.broker_futures_contract_data.get_actual_expiry_date_for_single_contract(contracts[0])



""" if __name__ == "__main__":

    config = Config()
    config = get_production_config()
    path = config.get_element("parquet_store")+'/'+CONTRACT_COLLECTION
    print("Get initial price data from IB")
    instrument_code = input("Instrument code? <return to abort> ")
    if instrument_code == "":
        exit()

    contracts= contract_list_from_instrument(instrument_code)
    for contract in contracts:
        print(contract)

    
    results = find_all_no_empty_parquet_files(path)
    print(len(results)) """