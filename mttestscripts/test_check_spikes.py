from sysdata.tools.manual_price_checker import *
from sysdata.data_blob import dataBlob
from sysproduction.data.broker import dataBroker
from sysobjects.contracts import futuresContract
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, HOURLY_FREQ
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from mtfuturesdata.mtMongoClient import mtMongoClient
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData
from sysproduction.update_historical_prices import write_merged_prices_for_contract
from sysobjects.futures_per_contract_prices import futuresContractPrices
import os
import pandas as pd
from syslogdiag.email_via_db_interface import send_production_mail_msg
from mttestscripts.files_tool import list_all_instruments_from_a_directory

def mt_report_price_spike(data, filename): 
    # SPIKE
    # Need to email user about this as will need manually checking
    msg = (
        "Spike found in prices for %s: need to manually check by running interactive_manual_check_historical_prices"
        % str(filename)
    )
    data.log.warning(msg)
    try:
        send_production_mail_msg(
            data, msg, filename
        )
    except BaseException:
        data.log.warning(
            "Couldn't send email about price spike for %s" % str(filename)
        )


def test_check_existing_parquet_for_spikes():
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")

    instrument_code = 'COFFEE'
    contract_date = '20250900'
    frequency = DAILY_PRICE_FREQ
    contract = futuresContract(instrument_code, contract_date)
    pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)

    old_data_passed = pst_prices.iloc[:1]
    new_data_passed = pst_prices.iloc[1:]
    result = manual_price_checker(old_data_passed, new_data_passed, max_price_spike=100)
    return result

def mt_test_price_spike_in_file(filename, column_to_check, delta_columns=''):
    old_data = pd.DataFrame()
    new_data = pd.read_parquet(filename)
    data = dataBlob(log_name="update_historical_prices")

    #if len(old_data) > 0:
    #    original_last_date_of_old_data = old_data.index[-1]
    #else:
    original_last_date_of_old_data = new_data.index[0]
    keep_older=True
    max_price_spike=100
    merged_data_with_status = full_merge_of_existing_data_no_checks(
                old_data, new_data, keep_older
            )

    merged_data_with_status = spike_check_merged_data(
            merged_data_with_status,
            column_to_check_for_spike=column_to_check,
            max_spike=max_price_spike,
        )
    spike_present = merged_data_with_status.spike_present
    if spike_present:
        mt_report_price_spike(data, filename)

def mt_test_all_parquet_in_a_folder(folder_name):
    all_parquets = list_all_instruments_from_a_directory(folder_name, extension='parquet')
    i = 0 
    for parquet_file in all_parquets: 
        full_file_path = os.path.join(folder_name, parquet_file)
        mt_test_price_spike_in_file(full_file_path, column_to_check='open', delta_columns='')
        i = i +1 
        print(i)



if __name__ == "__main__":
    #result = test_check_existing_parquet_for_spikes()
    #print(result)
    #result.to_csv('foo.csv')
    #mt_test_price_spike_in_file(filename='/mnt/sda1/data/parquet/futures_contract_prices/Day@COFFEE#20250900.parquet', column_to_check='OPEN', delta_columns='')
    mt_test_all_parquet_in_a_folder('/mnt/sda1/data/parquet/ib/RTH_1_day/')



### NEXT TO DO: BASICALLY RE-WRITE THE manual_price_checker to check SPIKE without manual interactive mode, only check for spikes for a file 
### from sysproduction.update_historical_prices.py to import the report spike method to send emails 