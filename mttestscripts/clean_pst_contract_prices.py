# 2025.09.04
# As noted https://www.notion.so/Data-Clean-Up-25e39604e82e805c8efdc33b7a2f8c2f?source=copy_link, my earlier data updating processes, in particular the one seeding data from IB, might need to be revisited
# As of 2025.09.04, I added the seeding data from IB code to ensure that the data is correctly populated from now on. 
# But earlier data might have the following issues: 1) "earlier" or partial bar data (which will affect H, L, C and Volume); 2) 0 volume bars in HOURLY data; 3) missing earlier data 
# The clean up procedures: 
# 1. remove all 0-volume bars from Hourly data; (and recompile the mixed frequency data if Hourly is changed)
# 2. For both hourly and daily data, update from the IB data if the PST contract key can be mapped to a unique IB conId. Still, will check for data consistency btwn two parquets before merging
# 2.a If there are more data in the merged dataset, then update the IB data parquet as well 
# 2.b If either hourly or daily data is changed, then recompile the mixed frequency data
# 3. Update FX data -- might as well just get a clean time series before backtest
import os
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
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, HOURLY_FREQ
from mtfuturesdata.mtMongoClient import mtMongoClient
from mttestscripts.ohlc_parquet_cleanup_tools import clean_up_ohlc


list_of_frequencies = [HOURLY_FREQ, DAILY_PRICE_FREQ]
pst_ohlvc_columns = ["OPEN", "HIGH", "LOW", "FINAL", "VOLUME"]  # PST format columns
ib_ohlvc_columns = ['open','high','low','close', 'volume', 'average']
pst_price_columns = ["OPEN", "HIGH", "LOW", "FINAL"]

###PST Instrument Data Cleaning Rules 
###These are similar to the IB OHLC data cleaning cases rules in ohlc_parquet_cleanup_tools.py
###1. KRWUSD, if the price value is greater than 0.01, scale it by 10**-3
###2. SUGAR_WHITE, if the price value is less than 100, scale it by 10 
PST_OHLC_Spike_Clean_Up_Instruments = [
    #{'contract_filter': 'KRWUSD', 'value_filter': 0.01, 'greater_than': True, 'scale_factor': 10**-3},
    #{'contract_filter': 'SUGAR_WHITE', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10},
    #{'contract_filter': 'JPY_mini', 'value_filter': 0.001, 'greater_than': False, 'scale_factor': 10**2},
    #{'contract_filter': 'JPY', 'value_filter': 0.001, 'greater_than': False, 'scale_factor': 10**2},
    #{'contract_filter': 'CAD2', 'value_filter': 50, 'greater_than': False, 'scale_factor': 10},
]    

#These rules are to be commented after being run once. Otherwise it will keep trimming data 
PST_OHLC_Spike_Clean_Up_Contracts_Remove_First_N_Lines = [
    #{'contract_filter': ('EUROSTX', '20170300', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('GAS_UK', '20020700', DAILY_PRICE_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('GASOILINE', '19970200', DAILY_PRICE_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('US30', '20201200', DAILY_PRICE_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('GBP_micro', '20090600', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('SARONA', '20230900', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('SHATZ', '20131200', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('V2X', '20121200', HOURLY_FREQ), 'lines_to_remove': 9},
    #{'contract_filter': ('V2X', '20130100', HOURLY_FREQ), 'lines_to_remove': 6},
    #{'contract_filter': ('V2X', '20130200', HOURLY_FREQ), 'lines_to_remove': 3},
    #{'contract_filter': ('V2X', '20130400', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('V2X', '20130700', HOURLY_FREQ), 'lines_to_remove': 2},
    #{'contract_filter': ('V2X', '20131000', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('V2X', '20160800', HOURLY_FREQ), 'lines_to_remove': 1}
    #{'contract_filter': ('BONO', '20210600', HOURLY_FREQ), 'lines_to_remove': 1}
    #{'contract_filter': ('EU-DJ-OIL', '20170600', HOURLY_FREQ), 'lines_to_remove': 9},
    #{'contract_filter': ('EUR_micro', '20090600', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('EURO600', '20161200', HOURLY_FREQ), 'lines_to_remove': 1},
    #{'contract_filter': ('EUROSTX', '20011100', DAILY_PRICE_FREQ), 'lines_to_remove': 1}
    
]    




#ensure that a time series dataframe has unique and monotonically increasing index 
def ensure_monotonic_and_unique_index_in_time_series(ohlc_data, keep_parameter='last'):
    if ohlc_data is None: 
        return ohlc_data
    if ohlc_data.empty: 
        return ohlc_data
    
    if not (isinstance(ohlc_data.index, pd.DatetimeIndex) or isinstance(ohlc_data.index, pd.PeriodIndex)):
        print('Warning: this is not a time series')
        return ohlc_data
    
    result = ohlc_data
    if not result.index.is_monotonic_increasing:
        df_sorted = result.sort_index(ascending=True)
        result = df_sorted

    if not result.index.is_unique:
        print('Warning: Data contains duplicated rows!!! ')
        #df_deduped = result.drop_duplicates(keep=keep_parameter)
        df_deduped = result[~result.index.duplicated(keep=keep_parameter)]
        result = df_deduped
    return result

def dedup_contract_prices(contract):
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")
    #print(contract)
    contract_price_modified = False
    for frequency in list_of_frequencies:
        if parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
            contract_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
            contract_prices_df = pd.DataFrame(contract_prices)

            cleaned_ohlc = ensure_monotonic_and_unique_index_in_time_series(contract_prices_df)

            if not contract_prices_df.equals(cleaned_ohlc):
                if len(contract_prices_df) > len(cleaned_ohlc):
                    print(f"Removed {len(contract_prices_df) - len(cleaned_ohlc)} duplicated bars for {contract}")
                else:
                    print(f"Data index is sorted now for {contract}")
                contract_price_modified = True
                contractPrices = futuresContractPrices(cleaned_ohlc)
                print('writing parquet for', contract, frequency, len(contractPrices))
                parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)

    if contract_price_modified:                
        print('writing merged contract')
        write_merged_prices_for_contract(data, contract, list_of_frequencies)

def dedup_all_pst_contract_prices():
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data = dataBlob(log_name="update_historical_prices")
    broker_data_source = dataBroker(data)
    diag_prices = diagPrices(data)
    
    for instrument_code in instruments: 
        #instrument_code ='SP500'
        price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))
        #print(price_dts)

        #contract_dates = list(set([contract[:6] for contract in list_of_contracts]))
        for contract_date in price_dts:
            contract = futuresContract(instrument_code, contract_date)
            dedup_contract_prices(contract)

#remove zero volume bars from PST futures contract prices data in a parquet file
#The frequency should not be changed from the default HOURLY_FREQ
#If there are no non-zero volume bars left, the hourly data will be deleted
#If hourly data is changed, the mixed frequency data will be recompiled
def remove_zero_volume_bars(contract, frequency=HOURLY_FREQ):
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")
    
    if parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
        contract_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
        cleaned_contract_prices = contract_prices.remove_zero_volumes()
        if len(contract_prices) > len(cleaned_contract_prices):
            if cleaned_contract_prices.empty:
                print(f"All data removed for {contract}, delete hourly?")
                parquet_price._delete_prices_at_frequency_for_contract_object_with_no_checks_be_careful(contract, frequency)
            else:
                print(f"Removed {len(contract_prices) - len(cleaned_contract_prices)} zero-volume bars for {contract}")
                parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, cleaned_contract_prices, frequency)

            write_merged_prices_for_contract(data, contract, list_of_frequencies)

#remove zero volume bars for all contracts in PST futures contract parquet store
def remove_zero_volume_bars_all_contracts(frequency=HOURLY_FREQ):
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data = dataBlob(log_name="update_historical_prices")
    diag_prices = diagPrices(data)

    for instrument_code in instruments: 
        price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))

        for contract_date in price_dts:
            contract = futuresContract(instrument_code, contract_date)
            remove_zero_volume_bars(contract, frequency)

#Get contract details from MongoDB from PST contract key
def get_contract_details_from_mongo(contract):
    config = Config()
    config = get_production_config()
    mongo_client = mtMongoClient() 
    mongo_client._set_db(config.get_element('legacy_ib_data_db'))
    contracts_collection_name = config.get_element('legacy_futures_contracts_collection')

    doc_filter = {'pst_contract_key': str(contract)}
    docs = list(mongo_client._generic_read_docs(contracts_collection_name, doc_filter))
    if len(docs)<1:
        print(f"No contract details found in MongoDB for {contract}")
        return None
    elif len(docs)>1:
        print(f"Multiple contract details found in MongoDB for {contract}, cannot proceed")
        return None
    contract_details = docs[0]

    return(contract_details)    

# Resolve IB contract price parquet filename from contract details and frequency
def resolve_ib_contract_price_filename_from_contract_details_and_frequency(contract_details, frequency):
    if contract_details is None:
        print("No contract details provided, cannot proceed")
        return None

    if '_id' not in contract_details:
        print(f"No conId found in contract details {contract_details}, cannot proceed")
        return None

    config = Config()
    config = get_production_config()
    parquet_home = config.get_element("parquet_ib_store")
    if frequency==DAILY_PRICE_FREQ:
        ts_collection_name = 'RTH_1_day'
    elif frequency==HOURLY_FREQ:
        ts_collection_name = 'CTH_1_hour'
    else:  
        print(f"Frequency {frequency} not supported for IB data, cannot proceed")
        return None
    
    conId = contract_details['_id']
    localSymbol = contract_details['localSymbol']
    ib_contract_price_file_name = os.path.join(parquet_home, ts_collection_name, localSymbol+"_"+str(conId)+".parquet")
    return ib_contract_price_file_name

#Get IB contract prices at frequency from contract details
def get_ib_prices_for_contract_details_at_frequency(contract_details, frequency):
    ib_contract_price_file_name = resolve_ib_contract_price_filename_from_contract_details_and_frequency(contract_details, frequency)
    if ib_contract_price_file_name is None or not os.path.exists(ib_contract_price_file_name):
        print(f"No IB price file found for contract details {contract_details} at frequency {frequency}")
        return None
    
    ib_contract_prices_at_frequency = pd.read_parquet(ib_contract_price_file_name)
    if ib_contract_prices_at_frequency is None or ib_contract_prices_at_frequency.empty:
        print(f"No IB prices found in file {ib_contract_price_file_name}")
        return None

    return ib_contract_prices_at_frequency 
    

def ib_ohlcv_df_to_pst_ohlcv_df(ib_ohlcv_df):
    if ib_ohlcv_df is None or ib_ohlcv_df.empty:
        print("No IB OHLCV data provided, cannot proceed")
        return None
    
    #drop the 'average' column if it exists, and rename the columns
    pst_ohlcv_df = ib_ohlcv_df.drop(columns=['average'], errors='ignore')

    pst_ohlcv_df = pst_ohlcv_df.rename(columns={
        'open': 'OPEN',
        'high': 'HIGH',
        'low': 'LOW',
        'close': 'FINAL',
        'volume': 'VOLUME',
        })

    pst_ohlcv_df.index.name = 'index'  # Ensure the index column is named 'index'
    pst_ohlcv_df.index = pst_ohlcv_df.index.tz_localize(None)

    return pst_ohlcv_df

def pst_ohlcv_df_to_ib_ohlcv_df(pst_ohlcv_df):
    if pst_ohlcv_df is None or pst_ohlcv_df.empty:
        print("No PST OHLCV data provided, cannot proceed")
        return None
    
    # Add a dummy 'average' column with NaN values
    

    ib_ohlcv_df = pst_ohlcv_df.rename(columns={
        'OPEN': 'open',
        'HIGH': 'high',
        'LOW': 'low',
        'FINAL': 'close',
        'VOLUME': 'volume',
        })
    ib_ohlcv_df['average'] = float('nan')

    ib_ohlcv_df = ib_ohlcv_df[['open', 'high', 'low', 'close', 'volume', 'average']]  # Reorder columns

    ib_ohlcv_df.index.name = 'date'  # Ensure the index column is named 'date'
    #set timezone of the ib_ohlcv_df index to UTC
    if ib_ohlcv_df.index.tzinfo is None:
        ib_ohlcv_df.index = ib_ohlcv_df.index.tz_localize('UTC')
    else:
        ib_ohlcv_df.index = ib_ohlcv_df.index.tz_convert('UTC')

    return ib_ohlcv_df

def check_pst_and_ib_data_consistency(pst_prices_df, ib_prices_df):
    if pst_prices_df is None or pst_prices_df.empty:
        print("No PST prices provided, cannot proceed")
        return False
    if ib_prices_df is None or ib_prices_df.empty:
        print("No IB prices provided, cannot proceed")
        return False
    
    # Find common indices (dates)
    # Then compare the open prices for overlapping dates 
    # Find the largest absolute percentage differences in open prices for overlapping dates
    # set a threshold for acceptable difference
    pst_prices_to_ib_df = pst_ohlcv_df_to_ib_ohlcv_df(pst_prices_df)

    common_indices = pst_prices_to_ib_df.index.intersection(ib_prices_df.index)
    if common_indices.empty:
        print("No common dates between PST and IB data, cannot check consistency")
        return False

    pst_common = pst_prices_to_ib_df.loc[common_indices]
    ib_common = ib_prices_df.loc[common_indices]

    # Calculate absolute percentage differences in open prices
    max_diff_open = 0 
    for column_name in ['open', 'high', 'low', 'close']:
        abs_percentage_diff_open = (pst_common[column_name] - ib_common[column_name]).abs() / ib_common[column_name].replace(0, float('nan')).abs() * 100

        if max_diff_open < abs_percentage_diff_open.max():
            max_diff_open = abs_percentage_diff_open.max()
            if max_diff_open > 0:
                #find the date of the max differences and return the data 
                idxmax = abs_percentage_diff_open.idxmax()
                print(f"Max absolute percentage difference in OPEN prices: {max_diff_open:.2f}% on {idxmax}, PST OPEN: {pst_common[column_name].loc[idxmax]}, IB OPEN: {ib_common[column_name].loc[idxmax]}") 
                results = {'max_diff': max_diff_open, 'date': idxmax, 'pst_open': pst_common[column_name].loc[idxmax], 'ib_open': ib_common[column_name].loc[idxmax], 'column_name': column_name}
                print(max_diff_open)
    if max_diff_open == 0: 
        results = True
    return results

def check_contract_and_ib_data_consistency_for_frequency(contract, frequency):
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")
    
    # get contract details from MongoDB from contract key
    contract_details = get_contract_details_from_mongo(contract)
    if contract_details is None:    
        print(f"No contract details found in MongoDB for {contract}, skip updating from IB")
        return False

    ib_prices_df = get_ib_prices_for_contract_details_at_frequency(contract_details, frequency)
    if ib_prices_df is None or ib_prices_df.empty:
        print(f"No IB prices found for {contract} at {frequency}, skip updating from IB")
        return False

    #If there is no pst data but there is ib data, then just write the ib data to pst parquet
    if not parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
        print(f"No existing {frequency} data for {contract}, skip updating from IB")
        return False

    pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
    # if pst data is None or empty then write ib data 
    if pst_prices is None or pst_prices.empty:
        return False
    
    # check for data consistency before merging
    pst_prices_df = pd.DataFrame(pst_prices)

    #check ib and pst data consistency
    pst_and_ib_consistent = check_pst_and_ib_data_consistency(pst_prices_df, ib_prices_df)
    if not isinstance(pst_and_ib_consistent, bool):
        
        return pst_and_ib_consistent
    else: 
        print(f"Data inconsistency found between PST and IB data for {contract} at {frequency}, skip updating from IB")
        return False
    #if not pst_and_ib_consistent:
    #    print(f"Data inconsistency found between PST and IB data for {contract} at {frequency}, skip updating from IB")
    #    return False

   
def check_all_pst_contract_parquet_and_ib_parquet_data_consistency():
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data = dataBlob(log_name="update_historical_prices")
    broker_data_source = dataBroker(data)

    update_outcome = pd.DataFrame()

    for instrument_code in instruments: 
        #instrument_code ='SP500'
        try:
            list_of_contracts = broker_data_source.get_list_of_contract_dates_for_instrument_code(instrument_code, allow_expired=True)
        except Exception as e:
            print(e)
            continue

        contract_dates = list(set([contract[:6] for contract in list_of_contracts]))
        for contract_date in contract_dates:
            contract = futuresContract(instrument_code, contract_date)
            for frequency in list_of_frequencies:
                update_results = check_contract_and_ib_data_consistency_for_frequency(contract, frequency)
                if not isinstance(update_results, bool):
                    update_results = {'contract': str(contract), 'frequency': frequency, **update_results}
                    print(update_results)
                    if update_outcome.empty:
                        update_outcome = pd.DataFrame([update_results])
                    else:
                        update_outcome = pd.concat([update_outcome, pd.DataFrame([update_results])] , ignore_index=True)

        #break
    
    if not update_outcome.empty:
        update_outcome_file = "pst_ib_data_update_outcome.csv"
        update_outcome.to_csv(update_outcome_file, index=False)
        print(f"Update outcome saved to {update_outcome_file}") 


def enhance_pst_data_with_ib_data_for_frequency(pst_data, ib_data, frequency): 
    #1. if the ib_data is empty, then return the pst_data
    #2. convert the ib_ data to pst format
    #3. if the pst_data is empty, then return with the ib_data converted to pst format
    #4. combine ib and pst data, with priority given to the ib_data, using combine_first
    #5. for daily data, ensure all hours are set to 23:00
    #6. return the result (leave the cleaning for the calling procedure)  
    if ib_data is None: 
        return pst_data
    
    if ib_data.empty: 
        return pst_data
    
    ib_data_to_pst = ib_ohlcv_df_to_pst_ohlcv_df(ib_data)

    if pst_data is None:
        return ib_data_to_pst

    if pst_data.empty: 
        return ib_data_to_pst
    
    #print(pst_data.head())
    #print(ib_data_to_pst.head())
    #print(len(pst_data))
    #print(len(ib_data_to_pst))
    
    if frequency == DAILY_PRICE_FREQ:
        result = ib_data_to_pst.combine_first(pst_data)
    else: 
        #for hourly frequency, we'll use the ib data, and patch any data that are earlier or later than existing ib data. 
        #the reason is that the hourly data may not always record at hourly points, some data are at HH:30 or HH:15 in one source but not at the other 
        #we'll have a lot of spurious data points if we simply combine two data sets, which will later on lead to other problems (e.g. bias in volatility estimation)
        ib_earliest_datetime = ib_data_to_pst.index[0]
        ib_latest_datetime = ib_data_to_pst.index[-1]
        earlier_pst_data = pst_data[pst_data.index < ib_earliest_datetime]
        #print(len(earlier_pst_data))
        later_pst_data = pst_data[pst_data.index > ib_latest_datetime]
        #print(len(later_pst_data))
        result = ib_data_to_pst
        if not earlier_pst_data.empty: 
            result = result.combine_first(earlier_pst_data)
        if not later_pst_data.empty: 
            result = result.combine_first(later_pst_data)
   
    #For daily data, set hours to 23:00 (some earlier pst data might have different hour values)
    if frequency == DAILY_PRICE_FREQ:
        result.index = result.index.map(lambda x: x.replace(hour=23, minute=0))

    result_cleaned = ensure_monotonic_and_unique_index_in_time_series(result)
    return result_cleaned

def enhance_pst_data_with_ib_data_for_contract(contract):
    #1. For each frequency: 
    #   1.a load the data
    #   1.b. call the data processing function
    #   1.c. apply pst cleaning tool 
    #   1.d. if data has been modified, write it to the parquet
    #2. If frequency data is changed, re-calculate the merged prices parquet
    #
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")
    contract_details = get_contract_details_from_mongo(contract)
    if contract_details is None:    
        print(f"No contract details found in MongoDB for {contract}, skip updating from IB")
        return

    price_data_changed  = False
    for frequency in list_of_frequencies:
        ib_prices_df = get_ib_prices_for_contract_details_at_frequency(contract_details, frequency)
        if ib_prices_df is None or ib_prices_df.empty:
            print(f"No IB prices found for {contract} at {frequency}, skip updating from IB")
            continue

        #If there is no pst data but there is ib data, then just write the ib data to pst parquet
        if not parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
            pst_prices_df = pd.DataFrame()
            pst_prices = None
        else:
            pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
            
            if frequency == HOURLY_FREQ: 
                cleaned_contract_prices = pst_prices.remove_zero_volumes()
                pst_prices_df = pd.DataFrame(cleaned_contract_prices)
            else: 
                pst_prices_df = pd.DataFrame(pst_prices)

        enhanced_data_cleaned = enhance_pst_data_with_ib_data_for_frequency(pst_prices_df, ib_prices_df, frequency)
        

        if pst_prices is None: 
            if enhanced_data_cleaned is None: 
                continue
            if enhanced_data_cleaned.empty: 
                continue
            price_data_changed = True
            contractPrices = futuresContractPrices(enhanced_data_cleaned)
            print('writing contract prices......' + str(contract) + ' ' + str(frequency))
            parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)
        else: 
            if enhanced_data_cleaned is None: 
                #delete parquet although it is highly unlikely
                parquet_price._delete_prices_at_frequency_for_contract_object_with_no_checks_be_careful(contract, frequency)
                price_data_changed = True
                continue
            if enhanced_data_cleaned.empty:
                #delete parquet although it is highly unlikely  
                parquet_price._delete_prices_at_frequency_for_contract_object_with_no_checks_be_careful(contract, frequency)
                price_data_changed = True
                continue

            pst_prices_df = pd.DataFrame(pst_prices)
            if not pst_prices_df.equals(enhanced_data_cleaned):
                price_data_changed = True
                contractPrices = futuresContractPrices(enhanced_data_cleaned)
                #print('writing contract prices......' + str(contract) + ' ' + str(frequency))
                parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)
                #print(pst_prices_df.head())
                #print(pst_prices_df.tail(10))
                #print(enhanced_data_cleaned.head())
                #print(enhanced_data_cleaned.tail(10))
                #print(len(pst_prices_df))
                #print(len(enhanced_data_cleaned))
                #pst_prices_df.to_csv('foo1.csv')
                #enhanced_data_cleaned.to_csv('foo2.csv')
                #print(enhanced_data_cleaned.index)
                #break

    if price_data_changed:
        print('writing merged prices.....')
        write_merged_prices_for_contract(data, contract, list_of_frequencies)
    return


def enhance_pst_dat_with_ib_data():
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    data = dataBlob(log_name="update_historical_prices")
    diag_prices = diagPrices(data)
    broker_data_source = dataBroker(data)

    for instrument_code in instruments: 
        price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))
        list_of_contracts = []
        try:
            list_of_contracts = broker_data_source.get_list_of_contract_dates_for_instrument_code(instrument_code, allow_expired=True)
        except Exception as e:
            print(e)
            continue

        if len(list_of_contracts) > 0:
            contract_dates = list(set([contract[:6]+'00' for contract in list_of_contracts]))
            price_dts = list(set(price_dts+contract_dates))

        for contract_date in price_dts:
            contract = futuresContract(instrument_code, contract_date)
            enhance_pst_data_with_ib_data_for_contract(contract)
    return

#Clean up data based on the rules defined above
#Scale values that are magnitudes off 
def cleanup_pst_parquets_by_rules():
    data = dataBlob(log_name="update_historical_prices")
    diag_prices = diagPrices(data)
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    
    #Scale value to fix magnitude mis-matches
    for rule in PST_OHLC_Spike_Clean_Up_Instruments: 
        instrument_code = rule['contract_filter']
        value_filter = rule['value_filter']
        greater_than = rule['greater_than']
        scale_factor = rule['scale_factor']
        
        price_dts = sorted(diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code))
        for contract_date in price_dts:
            contract = futuresContract(instrument_code, contract_date)
            price_data_changed = False
            for frequency in list_of_frequencies: 
                if parquet_price.has_price_data_for_contract_at_frequency(contract, frequency):
                    pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
                    pst_prices_df = pd.DataFrame(pst_prices)
                    cleaned_pst_prices_df= clean_up_ohlc(pst_prices_df, value_filter, greater_than, scale_factor, price_columns=pst_price_columns)
                    if not pst_prices_df.equals(cleaned_pst_prices_df):
                        #write the cleaned up df to parquet
                        #update price_parquet_update
                        price_data_changed = True
                        contractPrices = futuresContractPrices(cleaned_pst_prices_df)
                        print('writing contract prices......' + str(contract) + ' ' + str(frequency))
                        parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)
            
            if price_data_changed:
                print('writing merged prices.....')
                write_merged_prices_for_contract(data, contract, list_of_frequencies)

    #Remove the first couple of lines for certain contracts 
    for rule in PST_OHLC_Spike_Clean_Up_Contracts_Remove_First_N_Lines:
        instrument_code, contract_dt, frequency = rule['contract_filter']
        line_to_remove = rule['lines_to_remove']

        contract = futuresContract(instrument_code, contract_dt)
        pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
        pst_prices_df = pd.DataFrame(pst_prices)
        pst_prices_removed = pst_prices_df.iloc[line_to_remove:]
        contractPrices = futuresContractPrices(pst_prices_removed)
        print('writing contract prices......' + str(contract) + ' ' + str(frequency))
        parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)
        print('Write merged prices: ')
        write_merged_prices_for_contract(data, contract, list_of_frequencies)
    
    return





                        



if __name__ == "__main__":
    instrument_code = 'SP500'
    date_str = '20250900'
    contract = futuresContract(instrument_code, date_str)
    #remove_zero_volume_bars(contract)
    contract_details = get_contract_details_from_mongo(contract)
    #print(contract_details)

    dedup_all_pst_contract_prices()
    #Step 1: 
    #remove_zero_volume_bars_all_contracts()
    #Step 2: 
    #dedup_all_pst_contract_prices()
    #Step 3: 
    #check_all_pst_contract_parquet_and_ib_parquet_data_consistency()
    #Step 4: 
    #enhance_pst_dat_with_ib_data()
    #enhance_pst_data_with_ib_data_for_contract(contract)
    

    #Further Cleaning
    #Step 5: 
    #cleanup_pst_parquets_by_rules()

