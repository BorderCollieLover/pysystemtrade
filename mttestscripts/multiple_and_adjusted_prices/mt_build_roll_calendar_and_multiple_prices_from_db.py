#Experimenting building roll calendar and multiple prices from raw contract prices in parquet

import os
import pandas as pd
import numpy as np
from build.lib.sysinit.futures.build_roll_calendars import _create_approx_calendar_from_earliest_contract
from sysdata.tools.manual_price_checker import *
from sysdata.data_blob import dataBlob
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.parquet.parquet_access import EXTENSION as PARQUET_EXTENSION
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData, CONTRACT_COLLECTION,from_contract_and_freq_to_key,from_key_to_freq_and_contract
from sysproduction.data.broker import dataBroker
from sysproduction.update_historical_prices import write_merged_prices_for_contract
from sysproduction.data.prices import diagPrices
from sysobjects.contracts import futuresContract
from sysobjects.futures_per_contract_prices import futuresContractPrices
from sysobjects.dict_of_futures_per_contract_prices import dictFuturesContractFinalPrices
from sysobjects.multiple_prices import futuresMultiplePrices
from sysobjects.roll_parameters_with_price_data import find_earliest_held_contract_with_price_data, contractWithRollParametersAndPrices
from syscore.exceptions import missingData
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, HOURLY_FREQ, FUTURES_MONTH_LIST
from syscore.fileutils import get_resolved_pathname
from syscore.pandas.merge_data_keeping_past_data import _calculate_change_in_vol_normalised_units
from syslogdiag.email_via_db_interface import send_production_mail_msg
from mttestscripts.files_tool import list_all_instruments_from_a_directory
from mttestscripts.parquet.ohlc_parquet_cleanup_tools import ib_parquet_folders, find_files_by_filter, test_for_expiry_past_n_days, return_total_volume
from sysinit.futures.rollcalendars_from_db_prices_to_csv import build_and_write_roll_calendar
from sysdata.csv.csv_roll_parameters import csvRollParametersData

def from_contract_month_letter_to_contract_month_number(contract_month_letter: str) -> int:
    if contract_month_letter not in FUTURES_MONTH_LIST:
        raise ValueError(f"Invalid contract month letter: {contract_month_letter}")
    return FUTURES_MONTH_LIST.index(contract_month_letter) + 1

def from_roll_parameters_to_list_of_contract_months_str(roll_parameters):
    contract_months = []
    for month_letter in str(roll_parameters.priced_rollcycle):
        contract_months += [from_contract_month_letter_to_contract_month_number(month_letter)]
                            
    for month_letter in str(roll_parameters.hold_rollcycle):
        contract_months += [from_contract_month_letter_to_contract_month_number(month_letter)]
    
    contract_months = sorted(list(set(contract_months)))
    return ["{0:02d}".format(m) for m in contract_months]


def check_contract_dts(instrument_code, price_dts, frequency: Frequency):
    #This function returns the list of contract dts that have hourly prices: 
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)

    if frequency == HOURLY_FREQ:
        return [dt for dt in price_dts if parquet_price.has_price_data_for_contract_at_frequency(futuresContract(instrument_code, dt), frequency)]
    elif frequency == DAILY_PRICE_FREQ:
        return [dt for dt in price_dts if parquet_price.has_price_data_for_contract_at_frequency(futuresContract(instrument_code, dt), frequency)]
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")

def find_missing_contracts(existing_contract_dts, roll_parameters):
    #It will go through generate the expected contract dts based on the roll parameters, and compare with the existing contract dts

    if not existing_contract_dts:
        return []
    
    existing_contract_dts = sorted(existing_contract_dts)

    first_contract = existing_contract_dts[0]
    last_contract = existing_contract_dts[-1]
    first_year = int(first_contract[:4])
    last_year = int(last_contract[:4])

    contract_months_str = from_roll_parameters_to_list_of_contract_months_str(roll_parameters)
    expected_contract_dts = []

    for year in range(first_year, last_year + 1):
        for month in contract_months_str:
            contract_dt = f"{year}{month}00"
            month_end_date =pd.to_datetime( f"{year}-{month}-28")

            if month_end_date > pd.Timestamp.now():
                continue
            if first_contract <= contract_dt <= last_contract:
                expected_contract_dts.append(contract_dt)

    expected_contract_dts = sorted(expected_contract_dts)

    missing_contracts = [dt for dt in expected_contract_dts if dt not in existing_contract_dts]
    return missing_contracts

def check_for_missing_contracts(instrument_code: str, data: dataBlob):
    diag_prices = diagPrices(data)
    roll_parameters_data = csvRollParametersData()
    try:
        roll_parameters = roll_parameters_data.get_roll_parameters(instrument_code)
    except Exception as e:
        print(f"Error getting roll parameters for {instrument_code}: {e}")
        return

    #These are the existing contract codes for the instrument
    price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))
    for frequency in [DAILY_PRICE_FREQ, HOURLY_FREQ]:
        if frequency == HOURLY_FREQ:
            continue
        price_dts_with_freq = check_contract_dts(instrument_code, price_dts, frequency)
        missing_contracts_for_frequency = find_missing_contracts(price_dts_with_freq, roll_parameters)  
        if missing_contracts_for_frequency:
            print(f"Missing contracts for {instrument_code} at frequency {frequency}: {missing_contracts_for_frequency}")

    return 

if __name__ == "__main__":
    instrument_code = 'SP500'
    roll_calendar_output_path = '/mnt/sda1/data/futures/roll_calendar_csv'
    #build_and_write_roll_calendar(instrument_code, output_datapath='/mnt/sda1/data/futures/roll_calendar_csv', write=True, check_before_writing=True)
    #roll_parameters_data = csvRollParametersData()
    #roll_parameters = roll_parameters_data.get_roll_parameters(instrument_code)
    #print(roll_parameters)

    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data= dataBlob(log_name="Check-for-missing-contracts")

    for instrument_code in instruments:
        check_for_missing_contracts(instrument_code, data=data)

    """ with dataBlob(log_name="Build-Roll-Calendar-and-Multiple-Prices-from-DB") as data:
        parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
        parquet_futures_contract_price_data = parquetFuturesContractPriceData(parquet_access)
        diag_prices = diagPrices(data)
        list_of_instruments_all = diag_prices.get_list_of_instruments_with_contract_prices()
        print("List of all instruments with contract prices:")
        print(list_of_instruments_all)
        
        for instrument_code in list_of_instruments_all:
            print(f"Building roll calendar and multiple prices for {instrument_code}...")
            roll_parameters = None
            try: 
                roll_parameters = roll_parameters_data.get_roll_parameters(instrument_code)
            except Exception as e:
                continue
            build_and_write_roll_calendar(instrument_code, output_datapath=roll_calendar_output_path, write=True, check_before_writing=False)
            print(f"Roll calendar and multiple prices for {instrument_code} have been processed.") """
