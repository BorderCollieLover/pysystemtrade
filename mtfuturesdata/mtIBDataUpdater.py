from mtfuturesdata.mtIBData import mtIBData
from mtfuturesdata.mtMongoClient import mtMongoClient
from ib_insync import Future
from time import sleep
from sysdata.config.production_config import get_production_config, Config
import re
import datetime
import pytz
import pandas as pd
import pymongo
from syscore.pandas.merge_data_keeping_past_data import merge_newer_data_no_checks, OLD_DATA_ONLY, NEW_DATA_ONLY, MERGED_DATA, mergingDataWithStatus
import os 
from random import randint, shuffle
from enum import Enum

class SendHistBarQuery(Enum):
    INQUIRED = 1
    NO = 2

HISTQUERY_YES = SendHistBarQuery.INQUIRED
HISTQUERY_NO = SendHistBarQuery.NO


WAIT_FOR_A_LONG_TIME = 72
TOO_MANY_FAILED_DOWNLOADS = 20

class mtIBDataUpdater(mtIBData):
    def __init__(self):
        config = Config()
        config = get_production_config()
        super().__init__()
        self.mongo_client = mtMongoClient()
        self.mongo_client._set_db(config.get_element('mongo_ib_data_db'))
        self.mongo_client._generic_ensure_coll(config.get_element('mongo_ib_contract_collection'),coll_type='regular')
        self.contracts_collection_name = config.get_element('mongo_ib_contract_collection')
        self.parquet_home = config.get_element("parquet_ib_store")
        #self.mongo_db = self.mongo_conn[config.get_element('mongo_ib_data_db')]

    def Update_Contract_for_Frequency(self, contract, useRTH: bool='False',barSizeSetting: str='1 hour'):
        ...
        #0. add the contract to the contract database
        #1. decide collection name: RTH flag + barSizeSetting
        #2. ensure the collection exists 
        #3. Check if the contract exists in meta collection 
        #4. if exists, check if it is necessary to update 
        #5. if update-able, check if data exists, if so, retrieve the last data point 
        #6. retrieve data and update the collection with the information of last available data point
        #7. update house keeping information 

        #time series data: datetime, ohlcva (a for average)
        #meta data: conId
        #other data: 1) last update time; 2) time start, 3) time end 
        #return value indicates whether a historical bar enquery is sent to the IB API or no

        new_contract_flag = False
        doc_filter = {}
        ts_metadata = {}
        startDateTime =''
        last_update = ''
        expiry_dt = None
        endDateTime =''
        contract_expired = False
        DATA_INQ_STATUS = HISTQUERY_NO

        if not contract: 
            msg = ("Error:  Trying to add an empty contract to the contracts collection.")
            print(msg)
            return
        elif '_id' in contract.keys():
            doc_filter = {'_id': contract['_id']}
            docs = list(self.contracts_table.find(doc_filter))
            if len(docs)<1:
                self._add_one_contract_to_db(contract)
                new_contract_flag=True
        else:
            msg = ("Error: Contract (%s) doesn't have the _id field. Please use IB conId as _id." %(str(contract)))
            print(msg)
            return
        
        #create/check the existence of the time series collection in Mongo DB
        [ts_collection_name, ts_meta_name] = self.ensure_collection(useRTH, barSizeSetting)
        
        expiry_dt = self.convert_contract_expiry_to_datetime(contract)
        #requires a proper expiry date in the contract specification before proceeding
        if not expiry_dt:
            msg = ("Error: Contract (%s) doesn't have a proper expiry date." %(str(contract)))
            print(msg)
            return
        contract_expired = self.expired_contract(expiry_dt)

        if not new_contract_flag: 
            ts_metadata = self.metadata(ts_meta_name, doc_identifier=doc_filter)
            if ts_metadata: 
                [startDateTime, last_update, failed_attempts] = self.retrieve_update_info_from_meta(ts_metadata)
                if self.updated_recently (last_update):
                    #msg = ("Error: Historical bar at (%s) for contract (%s) is downloaded recently. Skipping." %(barSizeSetting, str(contract)))
                    #print(barSizeSetting + 'for this contract is downloaded recently. Skipping. ')
                    #print(msg)
                    return
                
                
                #For expired contracts, if attemps to retrieve data has exceeded a certain time, then return
                #When it comes to hourly and minute data, it is again unclear when the last trade is 
                if contract_expired:
                    if self.too_many_failed_downloads(failed_attempts):
                        #If there has been enough failed attempts on an expired contract, skip 
                        #msg = ("Too many errors downloading historical bar at (%s) for expired contract (%s). Skipping." %(barSizeSetting, str(contract)))
                        #print('Expired contract with too many download errors. Skipping.......')
                        #print(msg)
                        return
                    
        if contract_expired:
            #For expired contracts, use the day after expiry date as the end for data retrieval (as Hours , minutes and seconds are all 0s)
            endDateTime = expiry_dt + datetime.timedelta(days=1)
        else:
            endDateTime =''
            #print(endDateTime)
        
        ib_contract = self.contract_to_ibcontract(contract)
        #print(startDateTime)
        #print(endDateTime)
        ohlcv_data = self.retrieve_historical_data_for_contract_with_frequency(contract=ib_contract,
                                                                               startDateTime=startDateTime, 
                                                                               endDateTime=endDateTime, 
                                                                               useRTH = useRTH, 
                                                                               barSizeSetting = barSizeSetting)
        self.update_ts_data(contract, ohlcv_data, ts_collection_name, ts_meta_name, contract_expired, barSizeSetting)
        sleep(randint(0,150)/150) #pacing 
        return
    
    def update_ts_data(self, contract, ohlcv_data, ts_collection_name, ts_meta_name, contract_expired, barSizeSetting):
        meta_identifier = {'_id': contract['_id']} #used for identifying contract and the meta record in the ts_meta collection 
        meta_data = {'metadata': meta_identifier}


        if (ohlcv_data is None) or  ohlcv_data.empty: 
            dt = ''
        else:
            if barSizeSetting == '1 day':
                #for daily data, need to convert the date field from datetime.date to datetime.datetime
                #because mongodb allows only datetime for time series 
                #use 23:00 for daily data to be consistent with PST 
                ohlcv_data['date'] =  [x.replace(hour=23, minute=0) for x in pd.to_datetime(ohlcv_data['date'], yearfirst=True, format='%Y%m%d', utc=True)]
                #ohlcv_data['date'] = ohlcv_data['date'].tz_localize('UTC')
                #print(ohlcv_data)
            dt = ohlcv_data.loc[len(ohlcv_data)-1, 'date']
            ohlcv_data = ohlcv_data[['date','open','high','low','close', 'volume', 'average']]
            
            #this is the updating of the mongo ts data collection, which can be continued later 
            ## stop collecting data into MongoDB on August 22, 2024
            #ohlc_records_to_insert = [{**meta_data, **row} for row in ohlcv_data.to_dict('records')] #meta_data is defined above 
            #self.mongo_client._get_collection(ts_collection_name).insert_many(ohlc_records_to_insert)
            ## stop collecting data into MongoDB on August 22, 2024 

            #this is where the update parquet code should go
            #need to add a return status to see if there were new data being added. If not, it's essentially a retrieval error event 
            update_status = self.update_ts_to_parquet(contract, ohlcv_data=ohlcv_data, ts_collection_name=ts_collection_name)
            if (update_status.status == OLD_DATA_ONLY):
                dt = ''


            #end of else: where there is price data, update the mongo db or the parquet object 

        
        #update the meta collection: 
        self.update_ts_meta_data(contract, ts_meta_name=ts_meta_name, contract_expired=contract_expired,dt=dt)
        return
    
    def update_ts_meta_data(self, contract, ts_meta_name, contract_expired, dt):
        meta_identifier = {'_id': contract['_id']} #used for identifying contract and the meta record in the ts_meta collection 

        #Now insert/update meta data for the time series 
        #in addition to the _id field 
        #last_update_time: now()
        #last_date: if ohlcv_data is not empty
        #failed attempts : if expired and ohlcv data is empty
        #update_meta = {'$set':  {**update_time_start, **update_time_end} }
        #self.db_service._generic_update_one(coll_name=ts_meta_collection_name, doc_identifier=meta_identifier, update_instruction=update_meta, upsert=True)
        last_update_instr = {'last_update': datetime.datetime.now()}
        if dt!='':
            latest_datetime_instr = {'latest_datetime': dt}
            update_meta_instr = {'$set':  {**last_update_instr, **latest_datetime_instr} }
        else:
            update_meta_instr = {'$set':  {**last_update_instr} }
        self.mongo_client._generic_update_one(coll_name=ts_meta_name, doc_identifier=meta_identifier, update_instruction=update_meta_instr, upsert=True)

        meta_record = self.mongo_client._generic_read_docs(coll_name=ts_meta_name, doc_identifier=meta_identifier)[0]

        #need to check the logic here, seems to be buggy
        #dt == '' indicates no new data has been returned, hence the reason for record an ohlcv_error count for expired contracts
        #For not yet expired contracts, we'll ignore this because we don't want to "error" out far out futures that don't trade (actively) yet
        if contract_expired:
            if 'ohlcv_error' in meta_record.keys(): 
                if dt=='':
                    update_ohlcv_error_instr= {'$inc': {'ohlcv_error': 1}}
                else:
                    update_ohlcv_error_instr= {'$set': {'ohlcv_error': 0}}
            else:
                if dt=='':
                    update_ohlcv_error_instr= {'$set': {'ohlcv_error': 1}}
                else: 
                    update_ohlcv_error_instr = ''
            if isinstance(update_ohlcv_error_instr, dict):
                self.mongo_client._generic_update_one(coll_name=ts_meta_name, doc_identifier=meta_identifier, update_instruction=update_ohlcv_error_instr)
        return

    def update_ts_to_parquet(self, contract, ohlcv_data, ts_collection_name): 
        if ohlcv_data is None: 
            print('No ohlcv data, exit')
            return
        
        if len(ohlcv_data)<1: 
            print('No ohlcv data, exit')

        #check if there is existing parquet data
        parquet_obj_file = self.resolve_parquet_full_filename(contract, ts_collection_name)
        return(self.update_parquet_with_data(parquet_obj_file, ohlcv_data))

    def update_parquet_with_data(self, parquet_obj_file, ohlcv_data): 
        if os.path.exists(parquet_obj_file):
            old_data = pd.read_parquet(parquet_obj_file)
            #print(old_data)
            try: 
                old_data.index = old_data.index.tz_localize('UTC')
            except Exception as e: 
                #print(e)
                ...
        else:
            old_data = pd.DataFrame()
        new_data = ohlcv_data
        new_data.set_index('date', inplace=True)
        #print(old_data.tail())
        #print(new_data.tail())  
        #print(len(old_data ))
        #print(len(new_data))

        ## April 25, 2025
        ## This is where the merge happens in the parquet file. 
        ## It doesn't, however, replace the old data in the parquet file if new data is found for the same datetime index
        ## This is a problem because the downloading process might download partial data, particularly for daily data
        ## Thus it is better to use the full_merge function to ensure that the old data is replaced with the new data
        """ mergingDataWithStatus_obj = merge_newer_data_no_checks(old_data, new_data)
        merged_data = mergingDataWithStatus_obj.merged_data
        if (mergingDataWithStatus_obj.status == MERGED_DATA) or (mergingDataWithStatus_obj.status == NEW_DATA_ONLY):
            msg = ("Updating (%s) with %s new data points " %(parquet_obj_file, str(len(merged_data)-len(old_data))))
            print(msg)
            merged_data.to_parquet(parquet_obj_file)
 """
        #new code with replacing old data with new data
        merged_data  = new_data.combine_first(old_data)
        #print(len(old_data ))
        #print(len(merged_data))
        #print(old_data.tail())
        #print(merged_data.tail())
        #return(mergingDataWithStatus.only_old_data(old_data))
        if (old_data.equals(merged_data)):
            msg = ("No new data to update (%s) " %(parquet_obj_file))
            print(msg)
            return(mergingDataWithStatus.only_old_data(old_data))
        else:
            new_data_lines = len(merged_data)-len(old_data)
            if new_data_lines > 0:
                msg = ("Updating (%s) with %s new data points " %(parquet_obj_file, str(new_data_lines)))
            else:
                msg = ("Updating (%s) with new data points " %(parquet_obj_file))
            merged_data.to_parquet(parquet_obj_file)
            print(msg)
            return(mergingDataWithStatus.only_new_data(merged_data))


    def resolve_parquet_full_filename(self,contract, ts_collection_name):
        #the parquet object name for the raw data of a contract 
        #the full name include the path
        #the file should be stored in a sub-directory decided by ts_collection_name, e.g. cth_15_mins or rth_1_day
        #the file name is a string combining localSymbol, conId 
        #There are only 301 (as of 2024.08.18) local symbols that have duplicated contracts.
        #The duplication is no more than 2. 
        #However, for some Korean single stock futures, the only way to distinguish the two contracts are to use the conId
        parquet_store = self.parquet_home+"/"+ts_collection_name
        if not os.path.isdir(parquet_store):
            os.makedirs( parquet_store)
        return (parquet_store+"/"+contract['localSymbol']+"_"+str(contract['_id'])+".parquet")

    def contract_to_ibcontract(self, contract):
        return(Future(conId=contract['_id'],symbol=contract['symbol'],lastTradeDateOrContractMonth=contract['lastTradeDateOrContractMonth'],exchange=contract['exchange']))


    def ensure_collection(self, useRTH, barSizeSetting):
        ts_collection_name = re.sub('\s+','_', ('RTH' if useRTH else 'CTH')+'_'+barSizeSetting)
        ts_meta_name = ts_collection_name+'_meta'
        self.mongo_client._generic_ensure_coll(ts_collection_name)
        return([ts_collection_name, ts_meta_name])

    def too_many_failed_downloads(self,failed_attempts):
        if isinstance(failed_attempts,int):
            return (failed_attempts > TOO_MANY_FAILED_DOWNLOADS)
        else: 
            return False

    def convert_contract_expiry_to_datetime(self, contract):
        #convert a contract expiry string to a datetime.datetime object
        #the expiry comes in two formats, mostly in the format of 'YYYYMMHH' and some in the format of '20271119 16:30:00 GB'
        #For the latter, GB is not a proper time zone and needs to be replaced as GMT for python 
        expiry_str = contract['lastTradeDateOrContractMonth']
        if len(expiry_str) == 8:
            formatStr= '%Y%m%d'
        else:
            expiry_str  = expiry_str.replace(' GB', '')
            formatStr = '%Y%m%d %H:%M:%S'
        
        try:
            expiry_dt = datetime.datetime.strptime(expiry_str,formatStr)
            expiry_dt = expiry_dt.astimezone(tz=datetime.timezone.utc)
        except Exception as e: 
                print(e)
                msg = ("Error: contract expiry date is not recognized.")
                print(msg) 
                return None
        
        return expiry_dt



    def expired_contract(self, expiry_dt):
        #returns true if it has been at least 1 day past the expiry date
        # to ensure the contract has expired, not expiring 
        return((datetime.datetime.now(pytz.utc)-expiry_dt).days>=1)
        
    def updated_recently(self, last_update):
        if isinstance(last_update, datetime.datetime):
            hours_passed = (datetime.datetime.now() - last_update).days*24
            return(hours_passed < WAIT_FOR_A_LONG_TIME)
        else:
            return False

    def retrieve_update_info_from_meta(self, metadata):
        if ('latest_datetime' in metadata.keys()):
            latest_datetime = metadata['latest_datetime']
        else:
            latest_datetime = ''
        
        if ('last_update' in metadata.keys()):
            last_update_time = metadata['last_update']
        else:
            last_update_time = ''

        if ('ohlcv_error' in metadata.keys()):
            failed_attempts = int(metadata['ohlcv_error'])
        else:
            failed_attempts = ''
        
        return([latest_datetime, last_update_time, failed_attempts])
        
    def metadata(self, ts_collection_name, doc_identifier):
        docs = self.mongo._generic_read_docs(coll_name = ts_collection_name, doc_identifier=doc_identifier)
        if len(docs)<1:
            return {}
        else:
            return(docs[0])

    @property
    def mongo(self):
        return self.mongo_client
    
    @property
    def contracts_table(self):
        return self.mongo_client._get_collection(self.contracts_collection_name)

    def _add_one_contract_to_db(self, contract):
        #contract is a dictionary with the following information: 
        #{'_id': 530408530, 'currency': 'USD', 'exchange': 'CFE', 'lastTradeDateOrContractMonth': '20220401', 'localSymbol': 'IBHYJ2', 'multiplier': '1000', 'secType': 'FUT', 'symbol': 'IBXXIBHY', 'tradingClass': 'IBHY'}
        if not contract: 
            msg = ("Error:  Trying to add an empty contract to the contracts collection.")
            print(msg)

        keys_to_delete= ['most_recent_contract_tag', 'min_multiplier_tag', 'priority_tag', 'ohlcv_error']
        for key in keys_to_delete:
            contract.pop(key, None)
        if '_id' in contract.keys():
            try:
                self.contracts_table.insert_one(contract)
            except Exception as e: 
                if (e.details['code'] == 11000): 
                    msg = ("Warning:  Contract (%s) is already in the database" % (str(contract)) )
                else:
                    msg = ("Error: critial error trying to add contract (%s) to database. Error message: (%s)" %(str(contract), str(e)))
                print(msg)
        else:
            msg = ("Error: Contract (%s) doesn't have the _id field. Please use IB conId as _id." %(str(contract)))
            print(msg)
        return


def ensure_collection(useRTH, barSizeSetting):
    ts_collection_name = re.sub('\s+','_', ('RTH' if useRTH else 'CTH')+'_'+barSizeSetting)
    ts_meta_name = ts_collection_name+'_meta'
    #self.mongo_client._generic_ensure_coll(ts_collection_name)
    return([ts_collection_name, ts_meta_name])

def get_all_ib_contracts():
    config = Config()
    config = get_production_config()
    #print(config)
    #print(config.get_element("mongo_host"))
    #print(config.get_element("mongo_port"))
    #print(config.get_element("parquet_ib_store"))
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    testDB = testClient[config.get_element("legacy_ib_data_db")]
    testColl = testDB[config.get_element("legacy_futures_contracts_collection")]
    docs = list(testColl.find())
    #docs2= list(testColl.find({'ohlcv_error': {'$exists': False }}))
    #docs = list(testColl.find({}))
    #print(len(docs))
    #print(len(docs2))
    #docs = docs+ docs2
    print(len(docs))
    return(docs)

def one_contract_historical_ts_mongo_to_parquet(contract, barSizeSetting="15 mins", useRTH = False):
    config = Config()
    config = get_production_config()
    parquet_home = config.get_element("parquet_ib_store")
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    ibMongoDB = testClient[config.get_element('mongo_ib_data_db')]
    [ts_collection, ts_meta] = ensure_collection(useRTH=useRTH, barSizeSetting=barSizeSetting)
    ibMongoColl = ibMongoDB[ts_collection]

    #can look up meta data and proceed only when metadata shows that data exists 
    
    #retrieve time series data from mongo DB
    filter  = {"metadata._id": contract['_id']}

    data = pd.DataFrame(list(ibMongoColl.find(filter)))
    if (len(data) <1):
        return
    #data.drop(['metadata', '_id', 'barCount'], axis=1, inplace=True)
    if (barSizeSetting=='1 day'):
        #print(data)
        data['date'] = [x.replace(hour=23) for x in data['date']]
        

    data.set_index('date', inplace=True)
    data.sort_index(inplace=True)
    data = data[~data.index.duplicated(keep="first")]
    data = data[['open', 'high', 'low', 'close', 'volume', 'average']]
    #print(data)

    #resolve parquet object name
    parquet_store = parquet_home+"/"+ts_collection
    if not os.path.isdir(parquet_store):
        os.makedirs( parquet_store)
    parquet_object=  parquet_store+"/"+contract['localSymbol']+"_"+str(contract['_id'])+".parquet"

    #write data to parquet 
    data.to_parquet(parquet_object)

def export_all_existing_contract_prices():
    all_contracts = get_all_ib_contracts()
    download_settings = [
        {'useRTH': False, 'barSize':'5 mins'},
        {'useRTH': False, 'barSize':'15 mins'},
        {'useRTH': False, 'barSize':'1 hour'},
        {'useRTH': True, 'barSize':'1 day'}
        ]

    for download_setting in download_settings:
        for contract in all_contracts:
            one_contract_historical_ts_mongo_to_parquet(contract, barSizeSetting=download_setting['barSize'], useRTH= download_setting['useRTH'])
    return




if __name__ == "__main__":
    test_contracts = [
                    {'_id': 495512563, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20251219', 'localSymbol': 'ESZ5', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 515416632, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20261218', 'localSymbol': 'ESZ6', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 586139767, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20271217', 'localSymbol': 'ESZ7', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 4}, 
                    {'_id': 637533641, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20250919', 'localSymbol': 'ESU5', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 649180661, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20270617', 'localSymbol': 'ESM7', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 4}, 
                    {'_id': 649180666, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20280317', 'localSymbol': 'ESH8', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 649180671, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20260918', 'localSymbol': 'ESU6', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 649180675, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20280915', 'localSymbol': 'ESU8', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 1}, 
                    {'_id': 649180678, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20260618', 'localSymbol': 'ESM6', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 1}, 
                    {'_id': 649180681, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20270917', 'localSymbol': 'ESU7', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 7}, 
                    {'_id': 649180684, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20270319', 'localSymbol': 'ESH7', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 649180690, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20280616', 'localSymbol': 'ESM8', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 649180695, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20260320', 'localSymbol': 'ESH6', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 654503299, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20281215', 'localSymbol': 'ESZ8', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 4}, 
                    {'_id': 672387437, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20290316', 'localSymbol': 'ESH9', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
                    {'_id': 691171642, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20290615', 'localSymbol': 'ESM9', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 4}, 
                    {'_id': 711280049, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20290921', 'localSymbol': 'ESU9', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 4},
                    {'_id': 712984914, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20250731', 'localSymbol': 'SR1N5', 'multiplier': '4167', 'secType': 'FUT', 'symbol': 'SOFR1', 'tradingClass': 'SR1', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0},
    ] 
    test1_contracts = [
        {'_id': 495512563, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20251219', 'localSymbol': 'ESZ5', 'multiplier': '50', 'secType': 'FUT', 'symbol': 'ES', 'tradingClass': 'ES', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0}, 
        {'_id': 712984914, 'currency': 'USD', 'exchange': 'CME', 'lastTradeDateOrContractMonth': '20250731', 'localSymbol': 'SR1N5', 'multiplier': '4167', 'secType': 'FUT', 'symbol': 'SOFR1', 'tradingClass': 'SR1', 'most_recent_contract_tag': 2, 'min_multiplier_tag': 0, 'priority_tag': 2, 'ohlcv_error': 0},
    ]
    
    WAIT_FOR_A_LONG_TIME = 0
    test= mtIBDataUpdater()
    all_contracts = get_all_ib_contracts()            
    shuffle(all_contracts)                                                                      
    
    #for contract in all_contracts:
    for contract in test_contracts[:5]:
        #print(contract)
        test.Update_Contract_for_Frequency(contract, useRTH=False)
        test.Update_Contract_for_Frequency(contract, useRTH=False, barSizeSetting='15 mins')
        test.Update_Contract_for_Frequency(contract, useRTH=False, barSizeSetting='5 mins')
        test.Update_Contract_for_Frequency(contract, useRTH=True, barSizeSetting='1 day')
        #print(enquery_status)
        

    #export_all_existing_contract_prices()
    
    sleep(5)