#Import downloaded barchart data (2025 format) to the PST database 
import pandas as pd
from pandas.testing import assert_frame_equal 

from syscore.constants import arg_not_supplied
from syscore.dateutils import MIXED_FREQ, HOURLY_FREQ, DAILY_PRICE_FREQ
from syscore.fileutils import resolve_path_and_filename_for_package
from syscore.pandas.frequency import merge_data_with_different_freq
from sysdata.config.production_config import get_production_config
from sysdata.csv.csv_futures_contract_prices import ConfigCsvFuturesPrices, csvFuturesContractPriceData
from sysdata.data_blob import dataBlob
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.parquet.parquet_access import EXTENSION as PARQUET_EXTENSION
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData, CONTRACT_COLLECTION
from sysinit.futures.contract_prices_from_split_freq_csv_to_db import init_db_with_split_freq_csv_prices_for_code, write_prices_for_contract_at_frequency 
from sysobjects.contracts import futuresContract
from sysobjects.futures_per_contract_prices import futuresContractPrices
from sysproduction.data.prices import diagPrices
from sysproduction.update_historical_prices import write_merged_prices_for_contract

from mttestscripts.clean_pst_contract_prices import ensure_monotonic_and_unique_index_in_time_series

list_of_frequencies = [HOURLY_FREQ, DAILY_PRICE_FREQ]


diag_prices = diagPrices()
db_prices = diag_prices.db_futures_contract_price_data



##There is always a need to check that the data is consistent as bc-utils mappings can have errors. 

BARCHART_CONFIG = ConfigCsvFuturesPrices(
    input_date_index_name="Time",
    input_skiprows=0,
    input_skipfooter=0,
    #input_date_format="%Y-%m-%dT%H:%M:%S%z", 
    input_date_format="%Y-%m-%dT%H:%M:%S", 
    input_column_mapping=dict(
        OPEN="Open", HIGH="High", LOW="Low", FINAL="Close", VOLUME="Volume"
    ),
)

# assuming bc-utils config pasted into private
datapath = resolve_path_and_filename_for_package(
    get_production_config().get_element_or_default("barchart_path", None)
)

#Add new futures contract prices data 
#New data is in new_data_df
#PST existing data is in pst_data_df 
def add_new_futures_contract_prices_data(new_data: futuresContractPrices, pst_data: futuresContractPrices, frequency) -> futuresContractPrices:
    if (pst_data is None) or (pst_data.empty):
        return new_data
    
    if (new_data is None) or (new_data.empty): 
        return pst_data
    print(new_data.head())
    print(pst_data.head())
    
    combined_df = pst_data.combine_first(new_data)
    #For daily data, set hours to 23:00 (some earlier pst data might have different hour values)
    if frequency == DAILY_PRICE_FREQ:
        combined_df.index = combined_df.index.map(lambda x: x.replace(hour=23, minute=0))

    result_cleaned = ensure_monotonic_and_unique_index_in_time_series(combined_df)
    print(result_cleaned.head())
    return futuresContractPrices(result_cleaned)

def barchart_csv_to_parquet(barchart_data_path, parquet_path):
    data = dataBlob(log_name="update_historical_prices")
    parquet_access = ParquetAccess(parquet_path)
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    
    csv_prices = csvFuturesContractPriceData(barchart_data_path, config=BARCHART_CONFIG)
    barchart_instrument_codes = []
    for frequency in[HOURLY_FREQ, DAILY_PRICE_FREQ]: 
        instrument_codes = csv_prices.get_list_of_instrument_codes_with_price_data_at_frequency(frequency)
        barchart_instrument_codes += instrument_codes
    barchart_instrument_codes = sorted(list(set(barchart_instrument_codes)))
    print(barchart_instrument_codes)

    for instrument_code in barchart_instrument_codes: 
        for frequency in [DAILY_PRICE_FREQ, HOURLY_FREQ]:
            contract_dates_for_frequency = csv_prices.contract_dates_with_price_data_at_frequency_for_instrument_code(instrument_code, frequency)
            for contract_dt in contract_dates_for_frequency:
                contract = futuresContract(instrument_code, contract_dt)
                data = csv_prices.get_prices_at_frequency_for_contract_object(contract, frequency)
                parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, data, frequency)

    return 

#Add new futures contract prices data already in parquet format
#The existing PST parquet DB data will take precedence if there is overlapping data
def add_new_futures_contract_prices_parquet_data(new_data_path): 
    data = dataBlob(log_name="update_historical_prices")
    parquet_access = ParquetAccess(parquet_path)
    parquet_price = parquetFuturesContractPriceData(parquet_access)

    #temp output folder, for debugging only 
    #utput_path = '/mnt/sda1/tmp2'
    output_path = get_production_config().get_element("parquet_store")
    output_parquet_access = ParquetAccess(output_path)
    output_parquet_price = parquetFuturesContractPriceData(output_parquet_access)
    #output_csv_prices = csvFuturesContractPriceData(output_path)



    new_data_instrument_codes = []
    for frequency in list_of_frequencies: 
        instrument_codes = parquet_price.get_list_of_instrument_codes_with_price_data_at_frequency(frequency)
        new_data_instrument_codes += instrument_codes
        print(instrument_codes)
    split_frequency_instrument_codes = sorted(list(set(new_data_instrument_codes)))
    mixed_freq_instrument_codes = parquet_price.get_list_of_instrument_codes_with_price_data_at_frequency(MIXED_FREQ)
    mixed_freq_only_instrument_codes = list(set(mixed_freq_instrument_codes) - set(split_frequency_instrument_codes))

    for instrument in split_frequency_instrument_codes:
        print(instrument)
        split_contract_prices_changed = False
        for frequency in list_of_frequencies:
            contract_dts_for_frequency = parquet_price.get_prices_at_frequency_for_instrument(instrument, frequency)
            #print(frequency)
            for contract_dt in sorted(list(set(contract_dts_for_frequency))): 
                print(contract_dt)
                split_contract_prices_changed = False
                contract = futuresContract(instrument, contract_dt)
                new_data = parquet_price.get_prices_at_frequency_for_contract_object(contract, frequency)
                if db_prices.has_price_data_for_contract_at_frequency(contract, frequency):
                    pst_data = db_prices.get_prices_at_frequency_for_contract_object(contract, frequency)
                    ### combine pst_data with new_data, with pst_data taking precedence 
                    updated_data = add_new_futures_contract_prices_data(new_data, pst_data, frequency)
                    try: 
                        assert_frame_equal(pst_data, updated_data, check_dtype=False, check_index_type=False) # somehow the index.dtype are different for pst_data and updated_data for daily data, so use assert to check equality while explicitly ignoring dtypes (datetime64[us] vs. datetime64[ns])
                    except AssertionError: # the combined data is really different from the PST data 
                        output_parquet_price.write_prices_at_frequency_for_contract_object(contract, updated_data, frequency, ignore_duplication=True)
                        #output_csv_prices.write_prices_at_frequency_for_contract_object(contract, updated_data, frequency, ignore_duplication=True)
                        print('added data')
                        split_contract_prices_changed = True

                else:
                    output_parquet_price.write_prices_at_frequency_for_contract_object(contract, new_data, frequency, ignore_duplication=True)
                    #output_csv_prices.write_prices_at_frequency_for_contract_object(contract, new_data, frequency, ignore_duplication=True)
                    split_contract_prices_changed  = True

                if split_contract_prices_changed:
                    print('write merged prices ')
                    write_merged_prices_for_contract(data, contract, list_of_frequencies)
                #break
        #break

#export a 
def export_split_frequency_contract_prices_from_db(instrument, output_path):
    csv_prices = csvFuturesContractPriceData(output_path)
    db_prices = diag_prices.db_futures_contract_price_data

    for frequency in [HOURLY_FREQ, DAILY_PRICE_FREQ]:
        contract_dates_for_frequency = db_prices.get_prices_at_frequency_for_instrument(instrument, frequency)
        for contract_dt in contract_dates_for_frequency:
            contract = futuresContract(instrument, contract_dt)
            data = db_prices.get_prices_at_frequency_for_contract_object(contract, frequency)
            csv_prices.write_prices_at_frequency_for_contract_object(contract, data, frequency, ignore_duplication=True)

                
        
if __name__ == "__main__": 
    
    barchart_data_path = '/mnt/sda1/data/barchart2025'
    parquet_path = '/mnt/sda1/tmp'
    #barchart_csv_to_parquet(barchart_data_path, parquet_path)

    #parquet_path = '/mnt/sda1/data/parquet/futures_contract_not_in_barchart'
    #add_new_futures_contract_prices_parquet_data(parquet_path)

    #Note this process will export contract prices but in a format different from the bc-utils downloaded csv files.
    #It will hold the place but will certainly cause problem later when I try to reimport bc-utils data. 
    #If I really want to get data for these instruments I should just  sign up for a premium account and download the data in batch quickly 
    new_instruments3 = ["GASOILINE_ICE","INR",  "MSCIEMASIA", "INR-SGX"]
    csv_output_path = '/mnt/sda1/tmp2'
    for instrument_code in new_instruments3:
        export_split_frequency_contract_prices_from_db(instrument_code, csv_output_path)
