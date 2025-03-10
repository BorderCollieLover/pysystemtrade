#This is to be run daily, and updates only "active" contract data with multiple_prices files
# by calling functions from update_sampled_contracts and update_historical_prices
#Maybe I should use this code to update all contracts ..... 

from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from sysdata.data_blob import dataBlob
from sysproduction.update_sampled_contracts import  update_active_contracts_for_instrument
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysproduction.update_historical_prices import update_historical_prices
from mtfuturesdata.count_parquet_data import remove_empty_parquet
from mttestscripts.add_all_contracts_to_db import update_expiries_and_sampling_status_for_multiple_prices_contracts, remove_future_contracts_marked_as_not_sampling
from datetime import datetime

if __name__ == "__main__":
    FuturesInstrumentData = csvFuturesInstrumentData()
    config = Config()
    config = get_production_config()
    instruments = FuturesInstrumentData.get_list_of_instruments()
    #instruments = ['ALUMINUM']

    
    #add contracts to DB
    with dataBlob(log_name="Update-Sampled_Contracts") as data:
        for instrument in instruments:
            try:
                update_active_contracts_for_instrument(instrument,data)
            except Exception as e:
                print(e)

        
    #update all sampled contracts
    update_historical_prices()


    #update expiries and sampling status
    with dataBlob(log_name="Update-Sampled_Contracts") as data:
        for instrument in instruments:
            try:
                update_expiries_and_sampling_status_for_multiple_prices_contracts(data, instrument)
            except Exception as e:
                print(e)

    #remove empty parquet files
    path = config.get_element("parquet_store")+'/'+CONTRACT_COLLECTION+'/'
    remove_empty_parquet(path)

    #remove contracts that are far distant into the futures and are marked as not_sampling because IB doesn't have data for them yet
    current_year_month = datetime.now().strftime("%Y%m")
    remove_future_contracts_marked_as_not_sampling(current_year_month)
    #remove_futures_contracts_prior_to_197802()
    #remove_futures_contracts_without_prices()

    

    

    


