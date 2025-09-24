import os
import pandas as pd
import pickle 
import numpy as np
from sysdata.tools.manual_price_checker import *
from sysdata.data_blob import dataBlob
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.parquet.parquet_access import EXTENSION as PARQUET_EXTENSION
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData, CONTRACT_COLLECTION,from_contract_and_freq_to_key,from_key_to_freq_and_contract
from sysproduction.data.broker import dataBroker
from sysproduction.update_historical_prices import write_merged_prices_for_contract
from sysproduction.data.prices import diagPrices
from sysobjects.contracts import futuresContract
from sysobjects.futures_per_contract_prices import futuresContractPrices
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, HOURLY_FREQ
from syscore.pandas.merge_data_keeping_past_data import _calculate_change_in_vol_normalised_units
from syslogdiag.email_via_db_interface import send_production_mail_msg
from mttestscripts.files_tool import list_all_instruments_from_a_directory
from mttestscripts.ohlc_parquet_cleanup_tools import ib_parquet_folders, find_files_by_filter, test_for_expiry_past_n_days, return_total_volume
from mtfuturesdata.mtMongoClient import mtMongoClient


list_of_frequencies = [HOURLY_FREQ, DAILY_PRICE_FREQ]
max_price_spike=80
MINIMUM_ROWS_TO_CHECK_FOR_SPIKES = 10 

#These are files that can be skipped when checking for spikes 
no_spike_file_filters = [
    'AYV[FGHJKMNQUVXZ][0-9]_*.parquet',  #small values around 0
    'LT[FGHJKMNQUVXZ][0-9]_*.parquet',   #small values around 0
    'XZ[FGHJKMNQUVXZ][0-9]_*.parquet',    #Dividend futures that are intrinsically spikey 
    ## Below are futures that have no liquidity 
    'TIO[FGHJKMNQUVXZ][0-9]_*.parquet',  #Iron Ore CFR China Futures
    'H4[FGHJKMNQUVXZ][0-9]_*.parquet',   #New York Heating Day Degree Index
    'BB[FGHJKMNQUVXZ][0-9]_*.parquet',   #NYMEX Brent Financial Futures Index
    'BOS[FGHJKMNQUVXZ][0-9]_*.parquet',  #Boston Housing Index
    'CHI[FGHJKMNQUVXZ][0-9]_*.parquet',  #Chicago Housing Index
    'D0[FGHJKMNQUVXZ][0-9]_*.parquet',   #London Heating Day Degree Index
    'D2[FGHJKMNQUVXZ][0-9]_*.parquet',   #?
    'DEN[FGHJKMNQUVXZ][0-9]_*.parquet',  #Denver Housing Index
    'EVG[FGHJKMNQUVXZ][0-9]_*.parquet',  #China Evergrande Group stock futures (HKFE)
    'FDXD*_*.parquet',                   #DAX Dividend
    'FDVD*_*.parquet',                   #Dax Dividend
    'FDIV*_*.parquet',                   #FTSE MIB Dividend
    'LAX[FGHJKMNQUVXZ][0-9]_*.parquet',  #Los Angeles Housing Index
    'LAV[FGHJKMNQUVXZ][0-9]_*.parquet',  #Las Vegas Housing Index
    'MIA[FGHJKMNQUVXZ][0-9]_*.parquet',  #Miami housing index
    'NYM[FGHJKMNQUVXZ][0-9]_*.parquet',  #New York Housing Index
    'SDG[FGHJKMNQUVXZ][0-9]_*.parquet',  #San Diego Housing Index
    'SFR[FGHJKMNQUVXZ][0-9]_*.parquet',  #San Francisco Housing Index
    'WDC[FGHJKMNQUVXZ][0-9]_*.parquet',  #Washington DC Housing Index
    ##
]

#PST instruments and contracts to skip from spike detection: 
skip_instruments = ['MARS-ARGUS', 'PIPELINE', 'FTSE100-DIV', 'MIB-DIVI', 'HEAT-DEG-AMS','HEAT-DEG-LON', 'HEAT-DEG-NY', 'HOUSE-BO', 'HOUSE-CG', 'HOUSE-DC', 'HOUSE-DN', 'HOUSE-LA', 'HOUSE-LV', 'HOUSE-MI', 'HOUSE-NY', 'HOUSE-SD', 'HOUSE-SF', 'HOUSE-US']
skip_contracts = [futuresContract('CRUDE_ICE', '20200500'),futuresContract('CRUDE_W', '20200500'), futuresContract('CRUDE_W_mini', '20200500')]

#generate a list of files to skip when checking for spikes
def generater_spike_check_skip_list_for_ib_data():
    skip_list = []
    for no_spike_file_filter in no_spike_file_filters:
        for folder in ib_parquet_folders:
            skip_files = find_files_by_filter(folder, no_spike_file_filter)
            skip_list = skip_list + skip_files
    return skip_list


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


## Find the largest 10 spikes in a time series, using the vol and time normalized unit changes as defined by PST function _calculate_change_in_vol_normalised_units
def find_the_largest_spike(data):
    spikes = _calculate_change_in_vol_normalised_units(data)
    return(sorted(spikes,reverse=True)[:10])

##Find the largest n (default 10) spikes of a given column of an ohlc data 
def find_the_largest_spike_in_ohlc (ohlc_data, column_to_check): 
    return(find_the_largest_spike(ohlc_data[column_to_check]))

##Find the largest n (default 10) spikes of a given column of an OHLC parquet file
def find_the_largest_spike_in_an_parquet(parquet_file, column_to_check):
    ohlc_data = pd.read_parquet(parquet_file)
    return(find_the_largest_spike_in_ohlc(ohlc_data, column_to_check))

##Find the largest n (default 10) spikes of a given conlumn, of a contract in PST for a given frequency 
def find_the_largest_spike_in_a_contract_at_frequency(contract, frequency, column_to_check):
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")
    try:
        pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
        return(find_the_largest_spike_in_ohlc(pst_prices, column_to_check))
    except Exception as e: 
        print(e)
        return None

#Check if there is a spike in an OHLC, for a given column
#This method calls the PST's  data merging process to test for spikes, using a default max_price_spike threshold assigned above 
#To use the PST's data merging process, create an empty dataframe as the 'old_data', and use the ohlc as the 'new_data'
def mt_test_price_spike_in_ohlc(ohlc_data, column_to_check):
    if ohlc_data is None or ohlc_data.empty:
        return False
    
    if len(ohlc_data) < MINIMUM_ROWS_TO_CHECK_FOR_SPIKES:
        return False

    old_data = pd.DataFrame()
    new_data = ohlc_data
    
    #if len(old_data) > 0:
    #    original_last_date_of_old_data = old_data.index[-1]
    #else:
    keep_older=True
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
        #mt_report_price_spike(data, filename)
        return True
    else: 
        return False

#Test for spikes in all parquet files in a folder
#The column list is the list of columns to check, it should contain all price related columns (ohlc) for the particular data set. e.g. ['open', 'high', 'low', 'close'] for IB data, ['OPEN', 'HIGH', 'LOW', 'FINAL'] for PST
#The default behavior of PST price checker is to check the first column, usually the open 
def mt_test_spike_in_all_parquets_in_a_folder(folder_name, column_list=['open', 'high', 'low', 'close']):
    all_parquets = list_all_instruments_from_a_directory(folder_name, extension='parquet')
    data = dataBlob(log_name="update_historical_prices")

    manual_check_files = 'manual_check_files.pkl'
    i = 0 
    if os.path.exists(manual_check_files):
        with open(manual_check_files,'rb') as file:  
            files_to_manually_check = pickle.load(file)
    else:
        files_to_manually_check = []

    skip_list = generater_spike_check_skip_list_for_ib_data()

    for parquet_file in all_parquets: 
        full_file_path = os.path.join(folder_name, parquet_file)
        if full_file_path in files_to_manually_check:
            continue

        if not os.path.isfile(full_file_path):
            continue

        if full_file_path in skip_list:
            continue
        
        ohlc_data = pd.DataFrame()
        try:
            ohlc_data = pd.read_parquet(full_file_path)
        except Exception as e:
            print(e)
            continue

        if ohlc_data.empty: 
            continue

        if len(ohlc_data) < MINIMUM_ROWS_TO_CHECK_FOR_SPIKES:
            continue

        #print(full_file_path)
        #if the ohlc data's total volume is 0 and it has expired by at least 30 days, then skip as well 
        """ if return_total_volume(ohlc_data) == 0: 
            try:
                expired = test_for_expiry_past_n_days(ohlc_data)
            except Exception as e: 
                expired = False
            if expired: 
                continue """
    
        for column in column_list:
            try:
                spike_present = mt_test_price_spike_in_ohlc(ohlc_data, column_to_check=column)
            except Exception as e: 
                ...
            if spike_present: 
                try:
                    mt_report_price_spike(data, full_file_path)
                except Exception as e: 
                    print(e)
                files_to_manually_check = files_to_manually_check + [full_file_path]
                break
            #i = i +1 
            #print(i)

        with open (manual_check_files, 'wb') as file: 
            pickle.dump(files_to_manually_check, file)
    print(len(files_to_manually_check))

#Calls the manual_price_checker to fix an ohlc with spikes 
#It creates an empty old_data_passed dataframe, and use the ohlc to fix as the new_data_passed 
#returns the spike-fixed data
#It also uses a default max_price_spike value set above 
def mt_manual_check_spike_in_ohlc(ohlc_data, column_list=['open', 'high', 'low', 'close']):
    if ohlc_data is None: 
        return None
    
    if ohlc_data.empty: 
        return ohlc_data
    
    if len(ohlc_data) < MINIMUM_ROWS_TO_CHECK_FOR_SPIKES: 
        return ohlc_data
    
    data_columns = ohlc_data.columns 
    result = pd.DataFrame(columns=data_columns)
    #check all columns for spikes to correct
    for idx, column_name in enumerate(column_list):
        old_data_passed = pd.DataFrame(columns=column_list)
        if idx == 0:
            new_data_passed = ohlc_data     
            result = manual_price_checker(old_data_passed, new_data_passed, max_price_spike=max_price_spike, column_to_check = column_name)
            #When we pass an empty dataframe as the old_data_passed, the result will drop the first row of the new_data_passed
            #So we need to add the row back
            result = pd.concat([ohlc_data.head(1), result])
        else:
            #if it's not the first column to check, use the intermediate results from previous steps to retain any manual correction
            new_data_passed = result
            result = manual_price_checker(old_data_passed, new_data_passed, max_price_spike=max_price_spike, column_to_check = column_name)
            result = pd.concat([ohlc_data.head(1), result])
        #print(result.head())
    
    return result 

#use the PST manual_price_checker to fix spikes in a parquet
def mt_manual_check_spike_in_a_parquet(parquet_file, column_list=['open', 'high', 'low', 'close'], overwrite=False):
    ohlc_data = pd.read_parquet(parquet_file)
    spike_removed_data = mt_manual_check_spike_in_ohlc(ohlc_data, column_list)
    if overwrite:
        if not spike_removed_data.equals(ohlc_data):
            spike_removed_data.to_parquet(parquet_file)


def mt_manual_check_spike_in_a_file_list(filelist, column_list=['open', 'high', 'low', 'close'], overwrite=False):
    for file_name in filelist: 
        mt_manual_check_spike_in_a_parquet(file_name, column_list=column_list, overwrite=overwrite)


#Loop through a list of files with possible spikes and fix the potential time spikes 
def mt_manual_check_spike_in_ib_from_list():
    manual_check_list = '/mnt/sda1/manual_check_files.pkl'

    if os.path.exists(manual_check_list):
        with open(manual_check_list,'rb') as file:  
            files_to_manually_check = pickle.load(file)
    else:
        return

    while len(files_to_manually_check)> 0: 
        parquet_file = files_to_manually_check.pop()
        print(parquet_file)
        mt_manual_check_spike_in_a_parquet(parquet_file, overwrite=True)
        with open (manual_check_list, 'wb') as file: 
            pickle.dump(files_to_manually_check, file)
        #break

    return


def mt_test_spike_in_all_ib_parquets():
    folders = ['/mnt/sda1/data/parquet/ib/RTH_1_day/', '/mnt/sda1/data/parquet/ib/CTH_1_hour/', '/mnt/sda1/data/parquet/ib/CTH_15_mins/', '/mnt/sda1/data/parquet/ib/CTH_5_mins/']
    for folder in folders:
        mt_test_spike_in_all_parquets_in_a_folder(folder)

def test_for_spikes_from_daily_changes_and_intraday_variations(ohlc_data, daily_change_threshold=5, intraday_threshold=5, price_columns = ['open', 'high', 'low', 'close']):
    if ohlc_data is None: 
        return False
    
    if ohlc_data.empty: 
        return False
    
    if len(ohlc_data) < MINIMUM_ROWS_TO_CHECK_FOR_SPIKES:
        return False
    
    ohlc_price_data = ohlc_data[price_columns]
    
    ohlc_data_changes = ohlc_price_data.pct_change().abs()
    ohlc_data_changes['max'] = ohlc_data_changes.max(axis=1, skipna=True)
    if np.nanmax(ohlc_data_changes['max']) > daily_change_threshold:
        return True
    
    small_constant = 0.000001 
    ohlc_price_data = ohlc_price_data.abs()+ small_constant
    ohlc_price_data['max'] = ohlc_price_data.max(axis=1, skipna=True)
    ohlc_price_data['min'] = ohlc_price_data.min(axis=1, skipna=True)
    ohlc_price_data['MINMAX'] = ohlc_price_data['max']/ohlc_price_data['min']
    if max(ohlc_price_data['MINMAX']) > intraday_threshold:
        return True

    
    

def test_for_spikes2_for_a_parquet(parquet_file, daily_change_threshold=5, intraday_threshold=5, price_columns = ['open', 'high', 'low', 'close']):
    if not os.path.isfile(parquet_file):
        print('%s is not a file. ' %(str(parquet_file)))
        return 
    
    try:
        ohlc_data = pd.read_parquet(parquet_file)
    except Exception as e: 
        print (e)
        return
    
    spike_present = test_for_spikes_from_daily_changes_and_intraday_variations(ohlc_data, daily_change_threshold, intraday_threshold, price_columns)
    if spike_present:
        print(parquet_file)
    return spike_present



def test_for_spikes2_for_all_ib_parquets(daily_change_threshold=5, intraday_threshold=5, price_columns = ['open', 'high', 'low', 'close']):
    folders = ['/mnt/sda1/data/parquet/ib/RTH_1_day/', '/mnt/sda1/data/parquet/ib/CTH_1_hour/', '/mnt/sda1/data/parquet/ib/CTH_15_mins/', '/mnt/sda1/data/parquet/ib/CTH_5_mins/']
    skip_files = generater_spike_check_skip_list_for_ib_data()


    manual_check_list = 'manual_check_files.pkl'

    if os.path.exists(manual_check_list):
        with open(manual_check_list,'rb') as file:  
            files_to_manually_check = pickle.load(file)
    else:
        files_to_manually_check = []

    for folder in folders:
        all_parquets = list_all_instruments_from_a_directory(folder, extension='parquet')
        for parquet_file in all_parquets:
            
            full_parquet_path = os.path.join(folder, parquet_file)
            if full_parquet_path in files_to_manually_check:
                continue
            if full_parquet_path in skip_files:
                continue
            #print(full_parquet_path)
            spike_present = test_for_spikes2_for_a_parquet(full_parquet_path, daily_change_threshold, intraday_threshold, price_columns)
            if spike_present: 
                files_to_manually_check = files_to_manually_check + [full_parquet_path]
        with open (manual_check_list, 'wb') as file: 
            pickle.dump(files_to_manually_check, file)

def test_for_spikes_for_all_pst_parquets(price_columns = ['OPEN', 'HIGH', 'LOW', 'FINAL']):
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data = dataBlob(log_name="update_historical_prices")
    diag_prices = diagPrices(data)
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    #broker_data_source = dataBroker(data)
    pickle_file_name = 'pst_spike_filelist.pkl'
    pst_spike2_filelist = []
    
    for instrument_code in instruments: 
        if instrument_code in skip_instruments:
            continue
        price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))
        
        for contract_date in price_dts:
            contract = futuresContract(instrument_code, contract_date)
            if contract in skip_contracts:
                continue
            for frequency in list_of_frequencies: 
                if parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
                    pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
                    pst_prices_df = pd.DataFrame(pst_prices)

                    for column in price_columns:
                        try:
                            spike_present = mt_test_price_spike_in_ohlc(pst_prices_df, column_to_check=column)
                        except Exception as e: 
                            ...
                        if spike_present: 
                            ident = from_contract_and_freq_to_key(contract=contract, frequency=frequency)
                            filename = parquet_access._get_filename_given_data_type_and_identifier(CONTRACT_COLLECTION, ident)
                            print(filename)
                            pst_spike2_filelist += [filename]

                            try:
                                mt_report_price_spike(data, filename)
                            except Exception as e: 
                                print(e)
                            break
                        
    if len(pst_spike2_filelist)>0:
        with open(pickle_file_name,'wb') as pickle_file:
            pickle.dump(pst_spike2_filelist, pickle_file)

    return




def test_for_spikes2_for_all_pst_parquets(daily_change_threshold=5, intraday_threshold=5, price_columns = ['OPEN', 'HIGH', 'LOW', 'FINAL']):
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data = dataBlob(log_name="update_historical_prices")
    diag_prices = diagPrices(data)
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    #broker_data_source = dataBroker(data)
    pickle_file_name = 'pst_spike2_filelist.pkl'
    pst_spike2_filelist = []
    
    for instrument_code in instruments: 
        if instrument_code in skip_instruments:
            continue
        price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))
        
        for contract_date in price_dts:
            contract = futuresContract(instrument_code, contract_date)
            if contract in skip_contracts:
                continue
            for frequency in list_of_frequencies: 
                if parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
                    pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
                    pst_prices_df = pd.DataFrame(pst_prices)
                    spike_present = test_for_spikes_from_daily_changes_and_intraday_variations(pst_prices_df,daily_change_threshold=5,intraday_threshold=5,price_columns=price_columns)
                    if spike_present:
                        #rint('spike present for '+str(contract)+' for frequency ' + str(frequency))
                        ident = from_contract_and_freq_to_key(contract=contract, frequency=frequency)
                        filename = parquet_access._get_filename_given_data_type_and_identifier(CONTRACT_COLLECTION, ident)
                        print(filename)
                        pst_spike2_filelist += [filename]

    if len(pst_spike2_filelist)>0:
        with open(pickle_file_name,'wb') as pickle_file:
            pickle.dump(pst_spike2_filelist, pickle_file)

    return




def mt_manual_check_spike_in_pst_from_list():
    #1. Load the file list pickle
    #2. For each file, run the interactive fix process 
    #3. Update the mixed frequency contract 
    data = dataBlob(log_name="update_historical_prices")
    pickle_file_name = 'pst_spike2_filelist.pkl'
    if os.path.exists(pickle_file_name):
        with open(pickle_file_name,'rb') as file:  
            files_to_manually_check = pickle.load(file)
    else:
        return
    
    print(files_to_manually_check)

    while len(files_to_manually_check)> 0: 
        parquet_file = files_to_manually_check.pop()
        print(parquet_file)

        basename_with_ext = os.path.basename(parquet_file)
        basename_without_ext, _ = os.path.splitext(basename_with_ext)
        frequency, contract = from_key_to_freq_and_contract(basename_without_ext)
        if not (contract in skip_contracts):
        #print(frequency)
        #print(contract)
            mt_manual_check_spike_in_a_parquet(parquet_file, column_list=['OPEN', 'HIGH', 'LOW', 'FINAL'], overwrite=True)
            write_merged_prices_for_contract(data, contract, list_of_frequencies)
        with open (pickle_file_name, 'wb') as file: 
            pickle.dump(files_to_manually_check, file)
    return

        

    


if __name__ == "__main__":
    #This will go through all price parquets in the ib directory, and scan all ohlc columns for possible spikes. 
    #Files that need to be manually checked and corrected for spikes are saved in a pickle object
    #mt_test_spike_in_all_ib_parquets()

    #manually check one parquet file
    #parquet_file ='/mnt/sda1/data/parquet/ib/RTH_1_day/AFRQ6_399215311.parquet'
    #mt_manual_check_spike_in_a_parquet(parquet_file, overwrite=True)
    
    
    #these two functions scans all IB futures contract prices parquets and collect all files that might have a spike into a list, stored as a pickle on the hard drive
    #Better to run these two and fix the spikes separately.
    #test_for_spikes2_for_all_ib_parquets()
    #mt_test_spike_in_all_ib_parquets()

    #skip_files = generater_spike_check_skip_list_for_ib_data()
    #print(skip_files)
    #manually check and correct spikes in IB futures contract prices parquet, from the list generated above 
    #mt_manual_check_spike_in_ib_from_list()

    
    
    #Find spikes2 in PST Data
    #test_for_spikes2_for_all_pst_parquets()
    #test_for_spikes_for_all_pst_parquets()

    #Interactively fix spikes in pst
    mt_manual_check_spike_in_pst_from_list()



    ###################Below are just for testing purposes ############################
    #Sample: manually spike corrected data is returned, which can be used to over-write the original data file
    #result = test_check_existing_parquet_for_spikes()
    #print(result)
    #result.to_csv('foo.csv')


    #Below are checking the values of spikes 
    #parquet_file = '/mnt/sda1/data/parquet/ib/CTH_5_mins/RTYH5_672387509.parquet'
    #print(find_the_largest_spike_in_an_parquet(parquet_file, 'low'))

    #instrument_code = 'COFFEE'
    #contract_date = '20250900'
    #frequency = DAILY_PRICE_FREQ
    #contract = futuresContract(instrument_code, contract_date)
    #print(find_the_largest_spike_in_a_contract_at_frequency(contract, DAILY_PRICE_FREQ, 'OPEN'))


""" #This function is not used. It was just for testing
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
    result = manual_price_checker(old_data_passed, new_data_passed, max_price_spike=max_price_spike)
    return result

 """