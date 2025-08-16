#1. Get the final_contract, and create the required contract_chain
#2. From multiple_prices, get all contract expiry dates
#3. Combine the two to get the full contract_chain
#4. Add contracts to the database 
#5. Update each contract with the actual expiry date
#6. Set status all to sampling
#7. After sampling all data, update the sampling status as it should be 
from syscore.constants import success

from sysobjects.contract_dates_and_expiries import contractDate
from sysobjects.rolls import contractDateWithRollParameters
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION

from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices, get_valid_instrument_code_from_user
from sysproduction.data.contracts import dataContracts

from sysproduction.update_sampled_contracts import create_contract_object_chain_from_contract_date_chain, update_contract_database_with_contract_chain, ALL_INSTRUMENTS, update_expiries_and_sampling_status_for_contracts
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysproduction.update_historical_prices import update_historical_prices
from mtfuturesdata.count_parquet_data import remove_empty_parquet
import pymongo
import os
from datetime import datetime

def remove_future_contracts_marked_as_not_sampling(current_date_check_str):
    #this str should reflect the current date, although it is just a proximate date
    #current_date_check_str = '202501' #contracts with contract_date greater than this but marked as not_sampling will be removed 
    config = Config()
    config = get_production_config()
    
    #the futures_contract collection in production is hardcoded in sysdata.parquet.parquet_futures_per_contract_prices
    #contract_collection = CONTRACT_COLLECTION
    contract_collection = 'futures_contracts'
    mongoClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    contractDB = mongoClient[config.get_element('mongo_db')]
    contractCollection = contractDB[contract_collection] 
    contracts = contractCollection.find()
    contracts_not_sampling = [contract for contract in contracts if not contract['contract_params']['sampling']]
    future_contracts_not_sampling = [contract for contract in contracts_not_sampling if contract['contract_date_dict']['contract_list'][0]['contract_date']>=current_date_check_str]
    for test_contract in future_contracts_not_sampling:
        result=contractCollection.delete_one({'_id': test_contract['_id']}) 

def remove_futures_contracts_prior_to_197802():
    #the steps below might add too many contracts to the database, so we remove contracts that are too far into the past
    current_date_check_str = '197802' #contracts with contract_date greater than this but marked as not_sampling will be removed 
    config = Config()
    config = get_production_config()
    
    #the futures_contract collection in production is hardcoded in sysdata.parquet.parquet_futures_per_contract_prices
    contract_collection = 'futures_contracts'
    mongoClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    contractDB = mongoClient[config.get_element('mongo_db')]
    contractCollection = contractDB[contract_collection] 
    contracts = contractCollection.find({})
    for test_contract in contracts:
        if test_contract['contract_date_dict']['contract_list'][0]['contract_date']<=current_date_check_str:
            result=contractCollection.delete_one({'_id': test_contract['_id']}) 

def contract_key_to_parquet_file(contract_key):
    return (contract_key.replace('/','#')+'.parquet')   


#remove contracts that are in the database but don't have prices in the parquet store
#this is again because we might add too many contracts to the database
def remove_futures_contracts_without_prices():
    config = Config()
    config = get_production_config()
    
    #the futures_contract collection in production is hardcoded in sysdata.parquet.parquet_futures_per_contract_prices
    contract_collection = 'futures_contracts'
    path = config.get_element("parquet_store")+'/'+CONTRACT_COLLECTION+'/'


    mongoClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    contractDB = mongoClient[config.get_element('mongo_db')]
    contractCollection = contractDB[contract_collection] 
    contracts = contractCollection.find({})
    for test_contract in contracts:
        file_name = path+contract_key_to_parquet_file(test_contract['contract_key'])
        if not os.path.exists(file_name):
            print(file_name)
            #print('removing contract: ', test_contract['contract_key'])
            result=contractCollection.delete_one({'_id': test_contract['_id']}) 
        


def add_multiple_prices_contracts_to_db():
    """
    add all contracts from a muitiple_prices file to mongoDB, as it is necessary when initializing a system with stale historical data
    """
    with dataBlob(log_name="Update-Sampled_Contracts") as data:
        instrument_code = get_valid_instrument_code_from_user(
            allow_all=True, all_code=ALL_INSTRUMENTS
        )

        add_multiple_prices_contracts_to_db_for_instrument(data, instrument_code)   
        if instrument_code is ALL_INSTRUMENTS:
            return success

        do_another = True

        while do_another:
            EXIT_CODE = "EXIT"
            instrument_code = get_valid_instrument_code_from_user(
                allow_exit=True, exit_code=EXIT_CODE
            )
            if instrument_code is EXIT_CODE:
                do_another = False
            else:
                add_multiple_prices_contracts_to_db_for_instrument(data, instrument_code)   

def add_multiple_prices_contracts_to_db_for_instrument(data: dataBlob, instrument_code: str = ALL_INSTRUMENTS):
    contract_date_chain = create_full_contract_date_chain_for_multiple_prices(data, instrument_code)
    contract_object_chain = create_contract_object_chain_from_contract_date_chain(instrument_code, contract_date_chain)
    update_contract_database_with_contract_chain(instrument_code, contract_object_chain, data)
    update_expiries_and_sampling_status_for_contracts(instrument_code, data, contract_object_chain)
    return

def update_expiries_and_sampling_status_for_multiple_prices_contracts(data: dataBlob, instrument_code: str):   
    contract_date_chain = create_full_contract_date_chain_for_multiple_prices(data, instrument_code)
    contract_object_chain = create_contract_object_chain_from_contract_date_chain(instrument_code, contract_date_chain)
    update_expiries_and_sampling_status_for_contracts(instrument_code, data, contract_object_chain)
    return

def create_full_contract_date_chain_for_multiple_prices(data: dataBlob, instrument_code: str = ALL_INSTRUMENTS):
    diag_prices = diagPrices(data)
    multiple_prices = diag_prices.get_multiple_prices(instrument_code)
    current_contract_dict = multiple_prices.current_contract_dict()
    furthest_out_contract_date = current_contract_dict.furthest_out_contract_date()
    diag_contract = dataContracts(data)
    roll_parameters = diag_contract.get_roll_parameters(instrument_code)
    furthest_out_contract = contractDateWithRollParameters(contractDate(furthest_out_contract_date), roll_parameters)

    final_contract = furthest_out_contract.next_priced_contract()
    #create a basic date chain for recent expiries
    contract_date_chain = final_contract.get_contracts_from_recently_to_contract_date()
    print(contract_date_chain)

    #count the number of expiries in the multiprice file 
    num_of_contracts = len(list(set(multiple_prices['CARRY_CONTRACT']+multiple_prices['PRICE_CONTRACT']+multiple_prices['FORWARD_CONTRACT'])))
    estimated_num_of_contracts = len(contract_date_chain)+ num_of_contracts -1
    #print(estimated_num_of_contracts)

    #Use the above count to extend the date chain forward
    current_contract_date_with_roll_parameters = final_contract
    for i in range(2*estimated_num_of_contracts):
        current_contract_date_with_roll_parameters = (
                current_contract_date_with_roll_parameters.next_priced_contract()
            )
        current_contract_date = (
                current_contract_date_with_roll_parameters.contract_date
            )
        #print(current_contract_date )
        contract_date_chain.append(current_contract_date)

    #Use the above count to extend the date chain backward
    current_contract_date_with_roll_parameters = final_contract
    for i in range(2*estimated_num_of_contracts):
        current_contract_date_with_roll_parameters = (
                current_contract_date_with_roll_parameters.previous_priced_contract()
            )
        current_contract_date = (
                current_contract_date_with_roll_parameters.contract_date
            )
        #print(current_contract_date )
        contract_date_chain.append(current_contract_date)

    #print('Chain before checking data existence:')
    #print(contract_date_chain)

    #Now check if there are contract price data for are valid by checking if there are contract price data
    price_dts = diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code)
    #print(sorted(price_dts))
    max_price_dt = max(price_dts)
    min_price_dt = min(price_dts)
    new_contract_date_chains = []
    for contract_date in contract_date_chain:
        #print(contract_date)
        #f str(contract_date) in price_dts:
        if str(contract_date) <= max_price_dt and str(contract_date) >= min_price_dt:
            #print( str(contract_date) + ' in price_dts')
            new_contract_date_chains.append(contract_date)

    #print('Chain after checking data existence:')
    #print(new_contract_date_chains)

    return new_contract_date_chains


def update1() : 
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments()
    #add contracts to DB
    with dataBlob(log_name="Update-Sampled_Contracts") as data:
        for instrument in sorted(instruments):
            try:
                add_multiple_prices_contracts_to_db_for_instrument(data, instrument)
            except Exception as e:
                print(e)
    #update all sampled contracts
    update_historical_prices()

def update2(): 
    ############################################################
    #### Might need to manually check for spikes in the data first
    ############################################################
    #update expiries and sampling status
    config = Config()
    config = get_production_config()
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments()
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
    remove_futures_contracts_prior_to_197802()
    #remove_futures_contracts_without_prices()


if __name__ == "__main__":
    instrument_code = 'MILKWET'
    data = dataBlob(log_name="Update-Sampled_Contracts")
    contract_chain = create_full_contract_date_chain_for_multiple_prices(data, instrument_code)
    print(contract_chain)
    add_multiple_prices_contracts_to_db_for_instrument(data, instrument_code)
    update1()

    

    

    


