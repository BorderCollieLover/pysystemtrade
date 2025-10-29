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
from sysobjects.contracts import  listOfFuturesContracts
from sysobjects.contract_dates_and_expiries import expiryDate
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysdata.data_blob import dataBlob
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from sysproduction.data.prices import diagPrices, get_valid_instrument_code_from_user
from sysproduction.data.contracts import dataContracts
from sysproduction.update_sampled_contracts import create_contract_object_chain_from_contract_date_chain, update_contract_database_with_contract_chain, ALL_INSTRUMENTS, update_expiries_and_sampling_status_for_contracts
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData, CONTRACT_COLLECTION

import pandas as pd
import pymongo
import os
from datetime import datetime
from mtfuturesdata.count_parquet_data import remove_empty_parquet


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

def update_sampling_status_based_on_expiry_for_contracts(diag_contract: dataContracts, contract_objects_chain):
    for contract in contract_objects_chain:
        contract_to_check = diag_contract.get_contract_from_db(contract)
        #if not contract_to_check.currently_sampling:
        if not contract_to_check.expired():
            diag_contract.mark_contract_as_sampling(contract_to_check)
        else:
            diag_contract.mark_contract_as_not_sampling(contract_to_check)

def add_multiple_prices_contracts_to_db_for_instrument(data: dataBlob, instrument_code: str = ALL_INSTRUMENTS):
    diag_contract = dataContracts(data)
    #contracts that are already in the DB
    contracts_in_db = diag_contract.get_all_contract_objects_for_instrument_code(instrument_code)

    #add contracts that are not yet in the DB and update their sampling and expiries
    contract_date_chain = create_full_contract_date_chain_for_multiple_prices(data, instrument_code)
    contract_object_chain = create_contract_object_chain_from_contract_date_chain(instrument_code, contract_date_chain)
    new_contracts_to_add = [contract for contract in contract_object_chain if contract not in contracts_in_db]
    new_contracts_to_add = listOfFuturesContracts(new_contracts_to_add)
    update_contract_database_with_contract_chain(instrument_code, new_contracts_to_add, data)
    update_expiries_and_sampling_status_for_contracts(instrument_code, data, new_contracts_to_add)
    update_expiry_from_contract_price_data(diag_contract, new_contracts_to_add)

    #update sampling for all contracts based on expiry
    contracts_in_db = diag_contract.get_all_contract_objects_for_instrument_code(instrument_code)
    update_sampling_status_based_on_expiry_for_contracts(diag_contract, contracts_in_db)
    return

#Update expiry date in the production/futures_contracts database if there are historical contract prices 
#This will look for expired contracts that have historical contract prices data. 
#It will then compare the date of the last contract price data, if it is later than the expiry date in the database, update the expiry date to the later data. 
def update_expiry_from_contract_price_data(diag_contract, contract_objects_chain):
    #diag_contract = dataContracts(data)
    parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    parquet_price = parquetFuturesContractPriceData(parquet_access)
    for contract in contract_objects_chain:
        contract_to_check = diag_contract.get_contract_from_db(contract)
        if contract_to_check.expired():
            if parquet_price.has_merged_price_data_for_contract(contract_to_check):
                contract_prices = parquet_price._get_merged_prices_for_contract_object_no_checking(contract)
                price_data_last_dt = contract_prices.index[-1]
                if price_data_last_dt > contract_to_check.expiry_date:
                    price_data_last_dt = pd.Timestamp(price_data_last_dt).to_pydatetime()
                    diag_contract.update_expiry_date(contract_to_check, expiryDate(price_data_last_dt.year, price_data_last_dt.month, price_data_last_dt.day)) #update the new expiry to database 

#This updates expiry for all instruments and all expired contract in the database, using the latest contract price date, if available and later than the expiry date in the database 
def update_all_contract_expiry_dt_from_contract_price_data():
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments()
    data = dataBlob(log_name="Update-Sampled_Contracts")
    diag_contract = dataContracts(data)
    
    for instrument in instruments:
        contracts_in_db = diag_contract.get_all_contract_objects_for_instrument_code(instrument)
        update_expiry_from_contract_price_data(diag_contract, contracts_in_db)
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

    #Now check if there are contract price data for are valid by checking if there are contract price data
    price_dts = diag_prices.contract_dates_with_price_data_for_instrument_code(instrument_code)
    max_price_dt = max(price_dts)
    min_price_dt = min(price_dts)
    new_contract_date_chains = []
    for contract_date in contract_date_chain:
        #print(contract_date)
        #f str(contract_date) in price_dts:
        if str(contract_date) <= max_price_dt and str(contract_date) >= min_price_dt:
            #print( str(contract_date) + ' in price_dts')
            new_contract_date_chains.append(contract_date)
    #add all contracts with data to the contract chain as well 
    new_contract_date_chains = new_contract_date_chains + price_dts

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
    #update_historical_prices()


if __name__ == "__main__":
    instrument_code = 'NICKEL_LME'
    instrument_codes =['NICKEL_LME', 'WHEY', 'BITCOIN']
    data = dataBlob(log_name="Update-Sampled_Contracts")
    for instrument_code in instrument_codes: 
        ...
        #contract_chain = create_full_contract_date_chain_for_multiple_prices(data, instrument_code)
        #print(contract_chain)
        #print(len(contract_chain))
        #add_multiple_prices_contracts_to_db_for_instrument(data, instrument_code)


    #This will add contracts that are not yet in the databases, e.g. new contracts coming online, or new historical contracts downloaded from other sources 
    update1()
    #update_to_sample_for_unexpired_contracts_for_all_instruments()

    #This will update the expiry dates in the database, so they are not always point to the first day of the expiry month 
    #update_all_contract_expiry_dt_from_contract_price_data()

    

    

    


