from sysdata.data_blob import dataBlob
from sysproduction.data.contracts import dataContracts
from sysproduction.data.broker import dataBroker
from sysobjects.contracts import futuresContract
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ
from sysbrokers.IB.ib_futures_contracts_data import ibFuturesContractData
#from sysbrokers.IB.ib_futures_contract_price_data import ibFuturesContractPriceData
#from sysbrokers.IB.ib_instruments import ibInstrumentConfigData, futuresInstrumentWithIBConfigData
from sysbrokers.IB.client.ib_contracts_client import ibContractsClient
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from mtfuturesdata.mtIBFuturesContracts import ContractDetails2DataFrame
from mtfuturesdata.mtMongoClient import mtMongoClient
import pandas as pd
import pickle 
import os

#This script adds PST contract information to the IB Futures Contract database in MongoDB, initially populated outside of PST but has since been added to the PST codebase. 
#For each PST contract (identified as INSTRUMENT/YYYYMM00), the corresponding IB contract is retrieved. The detailed contract information, together with the PST contract key information, is populated to the IB contract database. 
#As the contract details are already populated in the IB contract database, the net effect is populating the IB contract database with the PST contract key information.
#This way we can establish the mapping between PST contracts and IB contracts quickly and without ambiguity

def add_PST_contract_info_to_IB_futures_contract_db():
    config = Config()
    config = get_production_config()
    mongo_client = mtMongoClient() 
    mongo_client._set_db(config.get_element('legacy_ib_data_db'))
    contracts_collection_name = config.get_element('legacy_futures_contracts_collection')
    
    FuturesInstrumentData = csvFuturesInstrumentData()
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    
    data = dataBlob(log_name="update_historical_prices")
    diag_contracts = dataContracts(data)
    broker_data_source = dataBroker(data)
    broker_future_contract_data = ibFuturesContractData(ibconnection=data.ib_conn, data=data)
    ib_contract_client = ibContractsClient(ibconnection=data.ib_conn, log=data.log)
    
    #list to store resolved contracts and the corresponding PST contract keys
    resolved_contracts = []
    pst_contract_keys = []

    #The dataframe created from the contract details, with a column for PST contract keys
    db_update_df = pd.DataFrame()

    instrument_pickle_file = 'processed_tickers.pkl'
    if os.path.exists(instrument_pickle_file):
        with open(instrument_pickle_file,'rb') as file:  
            processed_instruments = pickle.load(file)
    else:
        processed_instruments = []

    
    for instrument_code in instruments: 
        #instrument_code = 'SP500'
        if instrument_code in processed_instruments: 
            continue

        try:
            list_of_contracts = broker_data_source.get_list_of_contract_dates_for_instrument_code(instrument_code, allow_expired=True)
        except Exception as e:
            print(e)
            continue

        #there are occasional duplicates in the contract list
        net_list_of_contracts = list(set(list_of_contracts))
        if len(net_list_of_contracts) < len(list_of_contracts):
            print(f"%d Duplicate contract dates found for instrument {instrument_code}: {list_of_contracts} %({len(list_of_contracts) - len(net_list_of_contracts)} )    ")
        list_of_contracts = net_list_of_contracts


        resolved_contracts = []
        pst_contract_keys = []
        #all_contracts_list = diag_contracts.get_all_contract_objects_for_instrument_code(instrument_code)
        #print(all_contracts_list[-5:])
        
        for contract_date in list_of_contracts:
            date_str = contract_date[:6]
            contract_object = futuresContract(instrument_code, date_str)
            #print(contract_object)
            #print(str(contract_object)  )
            #print(contract_object.instrument)
            #print(contract_object.params )    
            #print(contract_object.expiry_date)
            #print(data.ib_conn)
            #broker_data_source.get_cleaned_prices_at_frequency_for_contract_object(contract_object, daily_freqency)
            #broker_futures_contract_price_data = ibFuturesContractPriceData(ibconnection=data.ib_conn, data=data)   
            #broker_futures_contract_price_data.get_prices_at_frequency_for_potentially_expired_contract_object(contract_object, daily_freqency)

            try:
                contract_object_with_ib_broker_config = broker_future_contract_data.get_contract_object_with_IB_data(contract_object, allow_expired=True)
                #print(contract_object_with_ib_broker_config.instrument)
                #print(contract_object_with_ib_broker_config.params )    
                #print(contract_object_with_ib_broker_config.expiry_date)
                #by now the IB contract details, including symbol, exchange and actual expiry date are available 
            
                ib_contract = ib_contract_client.ib_futures_contract(contract_object_with_ib_broker_config, allow_expired=True)
                ib_contract_details_list = ib_contract_client.ib.reqContractDetails(ib_contract)
                contract_details = ib_contract_details_list[0]
                #print(contract_details)
                resolved_contracts = resolved_contracts + [contract_details]
                pst_contract_keys = pst_contract_keys + [str(contract_object)]  
            except Exception as e: 
                print(e)
        
        if len(resolved_contracts)>0:
            contracts_df = ContractDetails2DataFrame(resolved_contracts)
            contracts_df['pst_contract_key'] = pst_contract_keys
            #update the futures contract collection in MongoDB, note this collection is not in the PST production database
            mongo_client._batch_update_doc_from_df_with_id(contracts_df,  ['_id'], contracts_collection_name)
            if db_update_df.empty:
                db_update_df = contracts_df
            else:
                db_update_df = pd.concat([db_update_df, contracts_df], ignore_index=True)
            #db_update_df.to_csv('updated_contracts_info.csv', index=False)
        
        #processed_instruments += [instrument_code]
        #with open (instrument_pickle_file, 'wb') as file: 
        #    pickle.dump(processed_instruments, file)

            
    
    return db_update_df




if __name__ == "__main__":
    contracts_df = add_PST_contract_info_to_IB_futures_contract_db()
    #if not contracts_df.empty:
    #    contracts_df.to_csv('updated_contracts_info.csv', index=False)

    