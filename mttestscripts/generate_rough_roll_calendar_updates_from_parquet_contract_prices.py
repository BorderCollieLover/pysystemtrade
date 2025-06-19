import os
from sysinit.futures.rollcalendars_from_db_prices_to_csv import build_and_write_roll_calendar, check_saved_roll_calendar
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from sysdata.config.production_config import get_production_config, Config
from glob import glob 
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData
from sysdata.parquet.parquet_access import ParquetAccess
from sysproduction.data.prices import diagPrices
from sysinit.futures.multipleprices_from_db_prices_and_csv_calendars_to_db import process_multiple_prices_single_instrument



def create_tmp_directories_for_update_roll_calendars():
    roll_calendars_from_db = os.path.join('data', 'futures', 'roll_calendars_from_db')
    if not os.path.exists(roll_calendars_from_db):
        os.makedirs(roll_calendars_from_db)

    multiple_prices_from_db = os.path.join('data', 'futures', 'multiple_from_db')
    if not os.path.exists(multiple_prices_from_db):
        os.makedirs(multiple_prices_from_db)

    spliced_multiple_prices = os.path.join('data', 'futures', 'multiple_prices_csv_spliced')
    if not os.path.exists(spliced_multiple_prices):
        os.makedirs(spliced_multiple_prices)
    return(roll_calendars_from_db, multiple_prices_from_db, spliced_multiple_prices)

#what I need to do is to create a temporary data directory to store futures contract price parquet files only for updating the existing roll calendar
#to do that, I need to: 
#1. create a temporary data directory
#2. For each instrument, examine the existing roll calendar
#3. Copy price files parquet for the instrument that are needed to update the roll calendar
#4. update the roll calendar 

#the tmp futures contract price parquets directory
#several thousands of parquet objects are to be copied to the temporary directory so it is better not to keep them in the data/futures directory in the repository 
#Note: here we create two folders: 
# The top level is tmp_futures_contract_price_parquets, which is similar to the parquet_store system parameter in PST
# Within this folder, a subfoler with the name equal to CONTRACT_COLLECTION (the value is futures_contract_prices) is created.
# The top level is passed on as an initialization parameter to a parquetAccess object, which in turn is used to initialize the parquetFuturesContractPriceData, 
#  which is passed on the build_and_write_roll_calendar function as the input_prices parameter.
# The relevant price parquet objects are copied to the CONTRACT_COLLECTION subfolder. This is just how PST code is setup. See notes on this topic :https://www.notion.so/Update-Roll-Calendars-17e39604e82e8077a4cacb64db102ae4
def prepare_tmp_futures_contract_parquets_folder_for_updating_roll_calendars():
    config = Config()
    config = get_production_config()
    tmp_futures_contract_price_parquets = os.path.join(config.get_element("parquet_store")+'/'+'tmp_futures_contract_price_parquets')
    if not os.path.exists(tmp_futures_contract_price_parquets):
        os.makedirs(tmp_futures_contract_price_parquets) 
    tmp_futures_contract_price_parquets_contract_collection = tmp_futures_contract_price_parquets + '/' + CONTRACT_COLLECTION
    if not os.path.exists(tmp_futures_contract_price_parquets_contract_collection):
        os.makedirs(tmp_futures_contract_price_parquets_contract_collection) 
    return tmp_futures_contract_price_parquets, tmp_futures_contract_price_parquets_contract_collection

def copy_futures_contract_price_parquets_for_roll_calendar(tmp_futures_contract_price_parquets_contract_collection):
    config = Config()
    config = get_production_config()
    #copy futures contract price parquets for the instrument to the temporary directory
    repo_roll_calendar_data = csvRollCalendarData()
    for instrument in repo_roll_calendar_data.keys():
        print(instrument)
        copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument, config, repo_roll_calendar_data,tmp_futures_contract_price_parquets_contract_collection)
    return

#For each instrument, find the last line in current roll calendar
#From this line, find the earliest contract used in this roll calendar, use this as the reference contract
#From the PST system futures contract prices repository (parquet_store  + CONTRACT_COLLECTION)
#Find all merged price parquet objects of this instrument, copy contracts that are no older than the reference contract to the temporary folder created above
def copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument, config, repo_roll_calendar_data,tmp_futures_contract_price_parquets_contract_collection):
    repo_roll_calendar = repo_roll_calendar_data.get_roll_calendar(instrument)
    end_date = repo_roll_calendar.index.max()
    #print(repo_roll_calendar.loc[repo_roll_calendar.index == end_date] )
    last_contract = min(   repo_roll_calendar.loc[repo_roll_calendar.index == end_date].values.flatten().tolist() )
    #print(instrument, last_contract)
    
    parquet_path = config.get_element("parquet_store")+'/'+CONTRACT_COLLECTION+'/'
    instrument_parquet_files = glob(parquet_path+instrument+"#*"+".parquet")
    contract_list = [os.path.basename(file).replace('.parquet', '').split('#')[1] for file in instrument_parquet_files]
    contracts_to_copy = [contract for contract in contract_list if contract >= str(last_contract)]

    contract_files_to_copy = [parquet_path+instrument+"#"+contract+ ".parquet" for contract in contracts_to_copy]
    for file in contract_files_to_copy:
        os.system('cp '+file+' '+tmp_futures_contract_price_parquets_contract_collection)

#This is a test function to check the parquet_futures_contract_price_data object and the data it contains
#This code is not used in the actual script
def test_parquet_futures_code():
    instrument_code = 'GAS_US_mini'
    #PST's default behavior is to start with the default parquet_future_contract_price_data object and the code below is just for checking and comparison
    #parquet_futures_contract_price_data, prices, dict_of_all_futures_contract_prices, dict_of_futures_contract_prices are based on the PST system folders where ALL historical futures contract prices are stored
    #They are not used in this script. 
    diag_prices = diagPrices()
    parquet_futures_contract_price_data = diag_prices.db_futures_contract_price_data
    prices = parquet_futures_contract_price_data 
    #The name of merged_prices_for_instrument is misleading. In the actual call to the parquet object to read files from the folder, a data_type parameter doubled as the subfolder name is passed to the function.
    #In the case of building roll calendars from futures contract prices, the data_type is CONTRACT_COLLECTION, which points to the futures_contract_prices subfolder in the parquet store
    #Retrieve a dictionary of OHLCV data for each contract, where the key of the dictionary is the contract ID, which is the expiry
    dict_of_all_futures_contract_prices = prices.get_merged_prices_for_instrument(instrument_code) 
    #print(dict_of_all_futures_contract_prices)
    #for key in dict_of_all_futures_contract_prices.keys():
    #    print(key, dict_of_all_futures_contract_prices[key])
    #Returns the final prices, or Close prices, C of the OHLCV data for each contract, again with the contract ID as the key in a dictionary format
    dict_of_futures_contract_prices = dict_of_all_futures_contract_prices.final_prices()
    #print(dict_of_all_futures_contract_prices)
    #for key in dict_of_futures_contract_prices.keys():
    #    print(key, dict_of_futures_contract_prices[key])



#Build all temporary roll calendars: 
if __name__ == "__main__":

    roll_calendars_from_db, multiple_prices_from_db, spliced_multiple_prices = create_tmp_directories_for_update_roll_calendars()

    tmp_futures_contract_price_parquets, tmp_futures_contract_price_parquets_contract_collection = prepare_tmp_futures_contract_parquets_folder_for_updating_roll_calendars()
    copy_futures_contract_price_parquets_for_roll_calendar(tmp_futures_contract_price_parquets_contract_collection)


    #This is where I create a tmp_parquet_futures_contract_price_data that points to the temporary directory where only futures contract prices since the last roll calendar line item is kept
    #The build_and_write_roll_calendar function will take the tmp_parquet_futures_contract_price_data object as an input. 
    #It will then generate roll calendars that can be patched to existing roll calendars 
    
    tmp_futures_contract_parquet_access = ParquetAccess(tmp_futures_contract_price_parquets)
    tmp_parquet_futures_contract_price_data = parquetFuturesContractPriceData(tmp_futures_contract_parquet_access)
    #print(tmp_parquet_futures_contract_price_data)
    #tmp_dict_of_all_futures_contract_prices = tmp_parquet_futures_contract_price_data.get_merged_prices_for_instrument( instrument_code )
    #tmp_dict_of_futures_contract_prices = tmp_dict_of_all_futures_contract_prices.final_prices()
    #print(tmp_dict_of_all_futures_contract_prices)


    repo_roll_calendar_data = csvRollCalendarData()
    for instrument in repo_roll_calendar_data.keys():
    #for instrument in ['GAS_US_mini']:
        try:
            #...
            #print(instrument)
            build_and_write_roll_calendar(instrument,input_prices=tmp_parquet_futures_contract_price_data, output_datapath=roll_calendars_from_db,check_before_writing=False)
        except Exception as e: 
            print(e)

        process_multiple_prices_single_instrument(instrument, csv_multiple_data_path=multiple_prices_from_db,  ADD_TO_DB=False, csv_roll_data_path=roll_calendars_from_db, ADD_TO_CSV=True)


