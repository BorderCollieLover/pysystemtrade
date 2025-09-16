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
from mttestscripts.ohlc_parquet_cleanup_tools import remove_duplicated_rows_from_ohlc
from sysproduction.data.prices import diagPrices


list_of_frequencies = [HOURLY_FREQ, DAILY_PRICE_FREQ]
pst_ohlvc_columns = ["OPEN", "HIGH", "LOW", "FINAL", "VOLUME"]  # PST format columns
ib_ohlvc_columns = ['open','high','low','close', 'volume', 'average']


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
            #cleaned_contract_prices_df = remove_duplicated_rows_from_ohlc(contract_prices_df)
            if not contract_prices_df.index.is_monotonic_increasing:
                print('Data dates are not increasing!!! '+str(contract)+str(frequency))
                df_sorted = contract_prices_df.sort_index(ascending=True)
                contract_prices_df = df_sorted
                print('writing sorted contract prices.......')
                contractPrices = futuresContractPrices(contract_prices_df)
                parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)
                contract_price_modified = True

            if not contract_prices_df.index.is_monotonic_increasing:
                print('Data dates are not increasing!!!')

            if not contract_prices_df.index.is_unique:
                print('Data contains duplicated rows!!! '+str(contract)+str(frequency))
                cleaned_contract_prices_df = contract_prices_df.loc[~contract_prices_df.index.duplicated(keep='first')]
                       
                if len(contract_prices_df) > len(cleaned_contract_prices_df):
                    print(f"Removed {len(contract_prices_df) - len(cleaned_contract_prices_df)} duplicated bars for {contract}")
                    contract_price_modified = True
                    contractPrices = futuresContractPrices(cleaned_contract_prices_df)
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
    broker_data_source = dataBroker(data)

    for instrument_code in instruments: 
        try:
            list_of_contracts = broker_data_source.get_list_of_contract_dates_for_instrument_code(instrument_code, allow_expired=True)
        except Exception as e:
            print(e)
            continue

        contract_dates = list(set([contract[:6] for contract in list_of_contracts]))
        for contract_date in contract_dates:
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

def update_parquet_contract_prices_from_ib_prices_df_for_frequency(contract, ib_prices_df, frequency):
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    data = dataBlob(log_name="update_historical_prices")
    
    if ib_prices_df is None or ib_prices_df.empty:
        print(f"No IB prices provided for {contract} at {frequency}, cannot proceed")
        return False

    pst_prices_df = ib_ohlcv_df_to_pst_ohlcv_df(ib_prices_df)
    if pst_prices_df is None or pst_prices_df.empty:
        print(f"Failed to convert IB prices to PST format for {contract} at {frequency}, cannot proceed")
        return False
    
    contractPrices = futuresContractPrices(pst_prices_df)
    print('writing parquet for', contract, frequency, len(contractPrices))
    #parquet_price._write_prices_at_frequency_for_contract_object_no_checking(contract, contractPrices, frequency)
    return True

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

    #print(pst_prices_df.head() )
    #print(ib_prices_df.head())
    #print(pst_prices_df.tail() )
    #print(ib_prices_df.tail())

    pst_prices_to_ib_df = pst_ohlcv_df_to_ib_ohlcv_df(pst_prices_df)

    common_indices = pst_prices_to_ib_df.index.intersection(ib_prices_df.index)
    if common_indices.empty:
        print("No common dates between PST and IB data, cannot check consistency")
        return False

    pst_common = pst_prices_to_ib_df.loc[common_indices]
    ib_common = ib_prices_df.loc[common_indices]

    #pst_common.reset_index(inplace=True)
    #ib_common.reset_index(inplace=True)

    # Calculate absolute percentage differences in open prices
    abs_percentage_diff_open = (pst_common['open'] - ib_common['open']).abs() / ib_common['open'].replace(0, float('nan')).abs() * 100

    max_diff_open = abs_percentage_diff_open.max()
    if max_diff_open > 0:
        #find the date of the max differences and return the data 
        idxmax = abs_percentage_diff_open.idxmax()
        print(f"Max absolute percentage difference in OPEN prices: {max_diff_open:.2f}% on {idxmax}, PST OPEN: {pst_common['open'].loc[idxmax]}, IB OPEN: {ib_common['open'].loc[idxmax]}") 
        results = {'max_diff': max_diff_open, 'date': idxmax, 'pst_open': pst_common['open'].loc[idxmax], 'ib_open': ib_common['open'].loc[idxmax]}
    else:
        results = {'max_diff': 0, 'date': 0, 'pst_open': 0, 'ib_open': 0}

    print(max_diff_open)


    # If all checks passed, the data is consistent
    return results

def update_contract_prices_from_ib_for_frequency(contract, frequency):
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
        return update_parquet_contract_prices_from_ib_prices_df_for_frequency(contract, ib_prices_df, frequency)

    pst_prices = parquet_price._get_prices_at_frequency_for_contract_object_no_checking(contract, frequency)
    # if pst data is None or empty then write ib data 
    if pst_prices is None or pst_prices.empty:
        return update_parquet_contract_prices_from_ib_prices_df_for_frequency(contract, ib_prices_df, frequency)
    
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
   
def update_pst_contract_prices_from_ib():
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
                update_results = update_contract_prices_from_ib_for_frequency(contract, frequency)
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

    
if __name__ == "__main__":
    instrument_code = 'SP500'
    date_str = '20250900'
    contract = futuresContract(instrument_code, date_str)
    #remove_zero_volume_bars(contract)
    contract_details = get_contract_details_from_mongo(contract)
    #print(contract_details)
    #get_ib_prices_for_contract_details_at_frequency(contract_details, HOURLY_FREQ)
    #get_ib_prices_for_contract_details_at_frequency(contract_details, DAILY_PRICE_FREQ)
    #print(resolve_ib_contract_price_filename_from_contract_details_and_frequency(contract_details, HOURLY_FREQ) )
    
    #update_contract_prices_from_ib_for_frequency(contract, HOURLY_FREQ)
    #update_contract_prices_from_ib_for_frequency(contract, DAILY_PRICE_FREQ)
    
    #update_pst_contract_prices_from_ib()
    
    
    dedup_all_pst_contract_prices()





    #remove_zero_volume_bars_all_contracts()


