import os
from matplotlib.pylab import record
import pymongo
from syscore.constants import arg_not_supplied
from sysdata.config.production_config import get_production_config, Config

class mtMongoClient():
    def __init__(self):
        config = Config()
        config = get_production_config()
        self.mongo_conn = pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))        
        
    def _set_db (self, db_name):
        self.mongo_db = self.mongo_conn[db_name]
        
    def _generic_ensure_coll(self, coll_name, coll_type="timeseries", granularity="hours" ):
        filter = {"name": {"$regex": r"^(?!system\.)"}}
        names = self.mongo_db.list_collection_names(filter=filter)
        exists = coll_name in names
        if not exists and (coll_type == "timeseries"):
            self.mongo_db.create_collection(
                coll_name, 
                timeseries={ 'timeField': 'date', 'metaField': 'metadata', 'granularity': granularity }, #note that here date is just the name of the timeField
                #check_exists=True
            )
            self.mongo_db.drop_collection("{}_meta".format(coll_name))
            self.mongo_db.create_collection("{}_meta".format(coll_name))
        if not exists and coll_type == "regular":
            self.mongo_db.create_collection(
                coll_name, 
                #check_exists=True
            )
        return True
    
    def _get_collection(self, coll_name):
        return(self.mongo_db[coll_name])
    
    def _batch_update_doc_from_df_with_id(self, df, id_fields=[], coll_name="foo"):
        if df.empty:
            print("Empty Dataframe to update in {}".format(coll_name))
            return
        
        if coll_name is None: 
            print("Collection Name is None.")
            return
        
        if (len(coll_name)==0):
            print("Collection Name is empty.")
            return
        
        if (id_fields is None):
            print("Dataframe ID fields are empty. Please specify. ")
            return
        
        if (len(id_fields) ==0):
            print("Dataframe ID fields are empty. Please specify. ")
            return
        
        # Add data validity check, i.e. id_fields are within the column headers 
        self._generic_ensure_coll(coll_name, coll_type="regular");
        if len(id_fields)==1 and id_fields[0]=='_id':
            ...
        else:
            df['_id'] = df[id_fields].astype(str).sum(1)

        for index, row in df.iterrows():
            try:
                record = row.to_dict()
                record_id = record.pop('_id') # Extract _id and remove from update fields
                # Define the fields to update (all fields except _id)
                update_fields = record
                self.mongo_db[coll_name].update_one({'_id': record_id}, {'$set': update_fields}, upsert=True)
            except Exception as e:
                print("Error updating record in {}: {}".format(coll_name, e))   


    #batch insert a DataFrame to MongoDB with specified id fields (columns)
    #if the id field has a column header _id, then use this field
    #otherwise, create a _id column by adding all fields in id fields 
    #need to add validity check, e.g. id_fields are a subset of DataFrame columns 
    def _batch_insert_doc_from_df_with_id(self,  df, id_fields = [], coll_name="foo"):
        if df.empty:
            print("Empty Dataframe to insert into {}".format(coll_name))
            return
        
        if coll_name is None: 
            print("Collection Name is None.")
            return
        
        if (len(coll_name)==0):
            print("Collection Name is empty.")
            return
        
        if (id_fields is None):
            print("Dataframe ID fields are empty. Please specify. ")
            return
        
        if (len(id_fields) ==0):
            print("Dataframe ID fields are empty. Please specify. ")
            return
        
        # Add data validity check, i.e. id_fields are within the column headers 
        self._generic_ensure_coll(coll_name, coll_type="regular");
        if len(id_fields)==1 and id_fields[0]=='_id':
            ...
        else:
            df['_id'] = df[id_fields].astype(str).sum(1)
        
        try:
            self.mongo_db[coll_name].insert_many(df.to_dict('records'), ordered=False)
        except pymongo.errors.BulkWriteError as e:
            panic = filter(lambda x: x['code'] != 11000, e.details['writeErrors'])
            if len(list(panic)) > 0:
                print ( "really panic", panic)
        return
    
    def _generic_read_docs(self, coll_name, doc_identifier={}):
        self._generic_ensure_coll(coll_name)
        docs = list(self.mongo_db[coll_name].find(doc_identifier))
        return(docs)
    
    def _generic_update_one(self, coll_name, doc_identifier={}, update_instruction={}, upsert=False):
        self._generic_ensure_coll(coll_name)
        doc_counts = self.mongo_db[coll_name].count_documents(doc_identifier)
        if doc_counts <1:
            if not upsert:
                print("Not found. Skip")
                return
        
        if doc_counts > 1 : 
            print("There are {} documents for filter {}, consider update_many.".doc_counts.doc_identifier)
            return
        try:
            self.mongo_db[coll_name].update_one(filter=doc_identifier, update=update_instruction, upsert=upsert )
        except Exception as e: 
            print(e)
        return
            
    def _generic_update_many(self, coll_name, doc_identifier={}, update_instruction={}, upsert=False):
        self._generic_ensure_coll(coll_name)
        try:
            self.mongo_db[coll_name].update(filter=doc_identifier, update=update_instruction, upsert=upsert )
        except Exception as e: 
            print(e)
        return


#this doesn't work. cannot change non-meta field of a time series 
def change_time_of_ts():
    config = Config()
    config = get_production_config()
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    testDB = testClient[config.get_element("mongo_ib_data_db")]
    testColl = testDB['RTH_1_day']
    testColl_records= list(testColl.find())
    print(len(testColl_records))
    print(testColl_records[0])
    doc_filter = {'_id': testColl_records[0]['_id']}
    old_date = testColl_records[0]['date']
    old_date.replace(hour=23)
    update_instr = {'$set': {'date': old_date} }
    testColl.update_one(doc_filter, update_instr)



## removing time series from the MongoDB as they are now stored in 
def delete_one_time_series(contract, ts_coll):
    doc_filter =  {"metadata._id": contract['_id']}
    ts_coll.delete_many(doc_filter)

#After exporting existing data to parquet, remove the time series from mongoDB to save space and save dumping output file size 
#I kept existing ES futures data just for future reference 
def delete_all_time_series():
    config = Config()
    config = get_production_config()
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    testDB = testClient[config.get_element("mongo_ib_data_db")]
    #contract_coll = testDB['Contracts']
    #all_contracts = list(contract_coll.find({}))
    #print(len(all_contracts))
    for ts_coll_name in [ 'RTH_1_day', 'CTH_1_hour', 'CTH_15_mins', 'CTH_5_mins']:
        ts_collection = testDB[ts_coll_name]
        meta_collection = testDB[ts_coll_name+'_meta']
        all_contracts = list(meta_collection.find({}))
        for contract in all_contracts:
            print('deleting ' + str(contract) + ' from ' + ts_coll_name)
            delete_one_time_series(contract, ts_collection)
            #export_contract_historical_ohlcv_to_parquet(contract, ts_collection, parquet_object_name)
        
if __name__ == "__main__":
    config = Config()
    config = get_production_config()
    path = config.get_element("parquet_store")
    print(config.get_element("mongo_host"))
    print(config.get_element("mongo_port"))
    testClient=pymongo.MongoClient(host=config.get_element("mongo_host"), port=config.get_element("mongo_port"))
    print(config.get_element("legacy_ib_data_db"))
    print(config.get_element("legacy_futures_contracts_collection"))
    testDB = testClient[config.get_element("legacy_ib_data_db")]
    testColl = testDB[config.get_element("legacy_futures_contracts_collection")]
    docs = list(testColl.find({'symbol': 'ES'}))
    print(len(docs))
    #remove some legacy fields that are no longer needed
    fields_to_remove=['most_recent_contract_tag','min_multiplier_tag', 'priority_tag' ]
    for field_to_remove in fields_to_remove:
        testColl.update_many(
            {},
            {"$unset": {field_to_remove: ""}}
        )
    #print(docs)

    

    
    

