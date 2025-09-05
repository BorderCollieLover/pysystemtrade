from time import sleep, time
import multiprocessing
from syslogging.logger import *
from mtfuturesdata.mtIBDataUpdater import mtIBDataUpdater, get_all_ib_contracts, ensure_collection, TOO_MANY_FAILED_DOWNLOADS
from random import randint, shuffle
from sysdata.config.production_config import get_production_config, Config
import pymongo
from datetime import date, datetime
from mtfuturesdata.count_parquet_data import count_all
from mtfuturesdata.mtIBFuturesContracts import mtIBContract

number_of_processes = 8
#A multiprocessing setup in PST, the basic example in this code works ok
mtIBDataUpdater_instance = None
def set_global_dataupdater_instance():
    global mtIBDataUpdater_instance 
    if not mtIBDataUpdater_instance: 
        sleep (randint(10,150))
        mtIBDataUpdater_instance = mtIBDataUpdater()

def update_ohlcv(effecttive_contract):
    mtIBDataUpdater_instance.Update_Contract_for_Frequency(**effecttive_contract) 

def update_all_ohlcv_contracts_serial(contracts):
    set_global_dataupdater_instance()
    shuffle(contracts)
    old_data_count = count_all()
    for contract in contracts: 
        update_ohlcv(contract)
    new_data_count = count_all()
    retrieved_data_points = new_data_count- old_data_count
    msg =  (f"Finished updating {len(contracts)} contracts on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, retrieved {retrieved_data_points} data points. The number of total data points is {new_data_count}.")
    print(msg)
    return retrieved_data_points

def update_all_ohlcv_contracts(contracts):
    with multiprocessing.get_context("spawn").Pool(initializer=set_global_dataupdater_instance, processes=number_of_processes) as pool:
        pool.map(update_ohlcv, contracts)
    pool.join()

def get_effective_contracts_for_frequency(useRTH=True, barSizeSetting='1 day'):
    [ts_coll_name, ts_meta_name] = ensure_collection(useRTH, barSizeSetting)
    contracts = get_all_ib_contracts()

    config = Config()
    config = get_production_config()
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    testDB = testClient[config.get_element("mongo_ib_data_db")]
    testColl = testDB[ts_meta_name]
    docs = list(testColl.find({'ohlcv_error': {'$gt': TOO_MANY_FAILED_DOWNLOADS }}))
    ids = [doc['_id'] for doc in docs]
    effective_contracts = [{'contract': contract, 'useRTH': useRTH, 'barSizeSetting': barSizeSetting} for contract in contracts if contract['_id'] not in ids]
    print(len(effective_contracts))
    return(effective_contracts)

def get_unsampled_contracts_for_frequency(useRTH=True, barSizeSetting='1 day'):
    [ts_coll_name, ts_meta_name] = ensure_collection(useRTH, barSizeSetting)
    contracts = get_all_ib_contracts()

    config = Config()
    config = get_production_config()
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    testDB = testClient[config.get_element("mongo_ib_data_db")]
    testColl = testDB[ts_meta_name]
    docs = list(testColl.find({'ohlcv_error': {'$gt': 0 }} and {'ohlcv_error': {'$lte': TOO_MANY_FAILED_DOWNLOADS }} ))
    print(len(docs))
    docs = list(testColl.find({'ohlcv_error': {'$gt': 0 }} and {'ohlcv_error': {'$lte': TOO_MANY_FAILED_DOWNLOADS }} and  {'latest_datetime': {'$exists': False}}))
    ids = [doc['_id'] for doc in docs]
    unsampled_contracts = [{'contract': contract, 'useRTH': useRTH, 'barSizeSetting': barSizeSetting} for contract in contracts if contract['_id'] in ids]
    print(len(unsampled_contracts))
    return(unsampled_contracts)
    

def get_all_effective_contracts():
    frequencies = [
            {'useRTH': False, 'barSizeSetting': '1 hour'},
            {'useRTH': False, 'barSizeSetting': '15 mins'},
            {'useRTH': False, 'barSizeSetting': '5 mins'},
            {'useRTH': True, 'barSizeSetting': '1 day'}
        ]
    effect_contracts = []

    for freq in frequencies:
        contracts = get_effective_contracts_for_frequency(**freq)
        effect_contracts = effect_contracts + contracts
    return effect_contracts

def get_all_unsampled_contracts():
    frequencies = [
            {'useRTH': False, 'barSizeSetting': '1 hour'},
            {'useRTH': False, 'barSizeSetting': '15 mins'},
            {'useRTH': False, 'barSizeSetting': '5 mins'},
            {'useRTH': True, 'barSizeSetting': '1 day'}
        ]
    unsampled_contracts = []

    for freq in frequencies:
        contracts = get_unsampled_contracts_for_frequency(**freq)
        unsampled_contracts = unsampled_contracts + contracts
    return unsampled_contracts

if __name__ == "__main__": 
    #unsampled_contracts = get_all_unsampled_contracts()
    #print(len(unsampled_contracts))
    #print(unsampled_contracts[:10])
    #exit(0)

    ibContractUpdater = mtIBContract()
    today = date.today()
    if today.weekday() == 5 or today.weekday() == 4:
        ibContractUpdater.generate_ib_futures_codes2()
        ibContractUpdater.resolve_ibfutures_contracts()


    multiprocessing.set_start_method("spawn")
    #contracts = get_all_ib_contracts()
    i = 0 
    while True:
        #update the contract database on Saturdays 
        start_time = time()
        effective_contracts = get_all_effective_contracts()
        print(len(effective_contracts))
        #old_data_count = count_all()
        
        #shuffle (unsampled_contracts)
        #update_all_ohlcv_contracts(unsampled_contracts)        
        shuffle(effective_contracts)
        update_all_ohlcv_contracts(effective_contracts)        
        
        #new_data_count = count_all()
        #retrieved_data_points = new_data_count- old_data_count
        #msg = ("Finished updating %s effective contracts on %s on run %s, retrieved %s data points. The number of all data points is %s. " %(str(len(effective_contracts)), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(i), str(retrieved_data_points), str(new_data_count))) 
        #print(msg)
        
        end_time = time()
        elapsed_time = end_time - start_time
        print(f"Finished updating, Elapsed time: {elapsed_time} seconds")
        if elapsed_time < 1800:
            break
        else: 
            sleep(900)
            i = i + 1
