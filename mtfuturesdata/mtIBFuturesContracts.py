from mtfuturesdata.mtIBData import mtIBData
from mtfuturesdata.mtMongoClient import mtMongoClient
from ib_insync import Future, util
from time import sleep
from sysdata.config.production_config import get_production_config, Config
import pandas as pd
from syscore.pandas.merge_data_keeping_past_data import merge_newer_data_no_checks, OLD_DATA_ONLY, NEW_DATA_ONLY, MERGED_DATA, mergingDataWithStatus
import os 
from enum import Enum
import glob

def ContractDetails2DataFrame(contract_details):
    if len(contract_details)>0: 
        contracts = [c.contract for c in contract_details  ]
        df1 = util.df(contracts)
        df1.rename(columns={'conId': '_id'}, inplace=True)
        df2 = util.df(contract_details)
        df2_new = df2.drop(columns=['contract'])
        full_df = pd.concat([df1, df2_new], axis=1 )

    return full_df

class mtIBContract(mtIBData):
    def __init__(self):
        config = Config()
        config = get_production_config()
        super().__init__()
        self.mongo_client = mtMongoClient()
        self.mongo_client._set_db(config.get_element('legacy_ib_data_db'))
        self.contracts_collection_name = config.get_element('legacy_futures_contracts_collection')
        self.codes_collection_name = config.get_element('legacy_futures_codes_collection')

    @property
    def mongo(self):
        return self.mongo_client
    

    def generate_ib_futures_codes2(self, path=["APFutures", 'NAFutures', 'EUFutures']):
        all_futures_codes = pd.DataFrame()
        for dir_name in path:
            full_path = os.path.join ('mtdatastore', 'IBContractCodes', dir_name)
            search_pattern = os.path.join(full_path, '*.csv')
            csv_files =  glob.glob(search_pattern)
            #print(csv_files)
            all_regional_futures = pd.DataFrame()
            for csv_file in csv_files: 
                data = pd.read_csv(csv_file, index_col=0, header=0)
                print(data)
                data.columns = ['symbol', 'product', 'ticker2', 'currency', 'instrument', 'region', 'exchange']
                if all_regional_futures.empty:
                    all_regional_futures = data
                else:
                    all_regional_futures = pd.concat([all_regional_futures, data], ignore_index=True)
                        
            if all_futures_codes.empty:
                all_futures_codes = all_regional_futures[['symbol', 'currency', 'exchange']]
            else:
                all_futures_codes = pd.concat([all_futures_codes,all_regional_futures[['symbol', 'currency', 'exchange']]] , ignore_index=True)
            
        all_futures_codes.drop_duplicates(ignore_index=True, inplace=True)
        self.mongo_client._batch_insert_doc_from_df_with_id( all_futures_codes, ['symbol', 'currency','exchange'], self.codes_collection_name)
            
        return all_futures_codes
    
    def resolve_ibfutures_contracts_for_one_code(self, futures_code):
        #This is based on earlier QuantDatabase code but is much simplified and use the ib_insync library instead of the native IB API
        #For each record: 
        # 1. Use the code to resolve contracts
        # 2. If successful, then add the resolved contracts to the database
        # 3. If not successful: do nothing 
        #print(futures_code)
                            
        ibcontract = Future()
        ibcontract.secType = "FUT"
        ibcontract.symbol= futures_code["symbol"]
        ibcontract.exchange= futures_code["exchange"]
        if not (str(futures_code['currency'])=='nan'): #some futures listings download from IB website directly are noisy and missing CURRENCY
        #should try to change this test to pd.isnull or pd.isna 
            ibcontract.currency = futures_code["currency"]
        #ibcontract.currency = futures_code["currency"]
        ibcontract.includeExpired = True
        resolved_ibcontracts=self.ib.reqContractDetails(ibcontract)
        sleep(1) # add a pacing, the resolve contract step is usually very fast and can finish within 10 minutes for all codes except those that cannot be resolved.
        
        #print(resolved_ibcontracts)
        
        if len(resolved_ibcontracts)>0:
            #create a dataframe from resolved contract details and insert them into the futures_ts_meta database
            #using conID as _id for MongoDB ensures no duplication of records 
            #print(resolved_ibcontracts)
            #print(len(resolved_ibcontracts))
            #contracts_df = pd.DataFrame([{'_id':c.contract.conId, 'currency':c.contract.currency, 'exchange':c.contract.exchange, 'lastTradeDateOrContractMonth':c.contract.lastTradeDateOrContractMonth, 'localSymbol':c.contract.localSymbol, 'multiplier':c.contract.multiplier, 'secType':c.contract.secType, 'symbol':c.contract.symbol, 'tradingClass':c.contract.tradingClass} for c in resolved_ibcontracts])
            contracts_df = ContractDetails2DataFrame(resolved_ibcontracts)
            #self.mongo_client._batch_insert_doc_from_df_with_id(contracts_df,  ['_id'], self.contracts_collection_name)
            self.mongo_client._batch_update_doc_from_df_with_id(contracts_df,  ['_id'], self.contracts_collection_name)
            print(contracts_df)
            #contracts_df.to_csv("{}.csv".format(ibcontract.symbol+ibcontract.exchange+ibcontract.currency)) #purely for debugging purpose
            
        return resolved_ibcontracts

    
    def resolve_ibfutures_contracts(self):
        #read all records from the Futures_Codes collection 
        #If no records, then return
        all_futures_codes = self.mongo_client._generic_read_docs(self.codes_collection_name)
        if all_futures_codes is None: 
            print("No futures codes found.")
            return(None)
        
        if len(all_futures_codes)==0:
            print("No futures codes found.")
            return([])
        
        for code in all_futures_codes:
            self.resolve_ibfutures_contracts_for_one_code(code)
            #break;
        
        return(all_futures_codes)

    


if __name__ == "__main__":
    test = mtIBContract()
    codes = test.generate_ib_futures_codes2()
    #print(codes)
    codes = test.resolve_ibfutures_contracts()

    #print(codes)
    #print(codes.iloc[0])
    #contracts = test.resolve_ibfutures_contracts_for_one_code(codes.iloc[0])
    #data = ContractDetails2DataFrame(contracts)
    #for index, row in data.iterrows():
    #    print(row.to_dict())