import os
import pandas as pd
from glob import glob 
import pickle 
from mttestscripts.roll_calendars.remove_spurious_roll import remove_spurious_roll_from_roll_calendar_data
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysdata.parquet.parquet_multiple_prices import parquetFuturesMultiplePricesData
from sysinit.futures.rollcalendars_from_db_prices_to_csv import build_and_write_roll_calendar, check_saved_roll_calendar
from sysinit.futures.multipleprices_from_db_prices_and_csv_calendars_to_db import process_multiple_prices_single_instrument
from sysobjects.multiple_prices import futuresMultiplePrices
from sysobjects.roll_calendars import rollCalendar
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from sysdata.config.production_config import get_production_config, Config
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData
from sysdata.parquet.parquet_access import ParquetAccess
from sysproduction.data.prices import diagPrices
from mttestscripts.files_tool import list_all_instruments_from_a_directory, backup_one_folder
from mttestscripts.roll_calendars.remove_spurious_roll import remove_spurious_roll_from_roll_calendar_data

def backup_repo_data():
    repo_paths = ['roll_calendars_csv', 'multiple_prices_csv', 'adjusted_prices_csv', 'fx_prices_csv']

    config = Config()
    config = get_production_config()
    parquet_path = config.get_element("parquet_store")
    repo_back_up_root = os.path.join(parquet_path, '..', 'repo_backup')

    if not os.path.exists(repo_back_up_root):
        os.makedirs(repo_back_up_root)

    date_str = pd.Timestamp.now().strftime('%Y%m%d')
    repo_back_up_path = os.path.join(repo_back_up_root, date_str)
    if not os.path.exists(repo_back_up_path):
        os.makedirs(repo_back_up_path)
    
    for repo_path in repo_paths:
        source_folder = os.path.join('data', 'futures', repo_path)
        backup_folder = os.path.join(repo_back_up_path, repo_path)
        print(source_folder)
        backup_one_folder(source_folder, backup_folder, extension='.csv')
        print(f"Backed up {repo_path} to {backup_folder}")

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

def copy_futures_contract_price_parquets_for_roll_calendar(tmp_futures_contract_price_parquets_contract_collection):
    config = Config()
    config = get_production_config()
    #copy futures contract price parquets for the instrument to the temporary directory
    repo_roll_calendar_data = csvRollCalendarData()
    print(repo_roll_calendar_data.get_list_of_instruments() )
    print(repo_roll_calendar_data)
    for instrument in repo_roll_calendar_data.keys():
    #for instrument in ['LEAD_LME', 'TIN_LME', 'ZINC_LME']:
        print(instrument)
        copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument, config, repo_roll_calendar_data,tmp_futures_contract_price_parquets_contract_collection)
    return

#For each instrument, find the last line in current roll calendar
#From this line, find the earliest contract used in this roll calendar, use this as the reference contract
#From the PST system futures contract prices repository (parquet_store  + CONTRACT_COLLECTION)
#Find all merged price parquet objects of this instrument, copy contracts that are no older than the reference contract to the temporary folder created above
#However, since the roll calendar's last line is usually a spurious entry, we actually go back one more line in the roll calendar
#We copy the data to a temp folder and build the roll calendars as we only want to generate roll calendars for the recent time periods to update the existing repo multiple prices
def copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument, config, repo_roll_calendar_data,tmp_futures_contract_price_parquets_contract_collection):
    repo_roll_calendar = repo_roll_calendar_data.get_roll_calendar(instrument)
    #end_date = repo_roll_calendar.index.max()
    #This is assuming that the roll calendar does not contain a spurious roll, e.g. backed out from multiple prices
    #Need to go back one more line so as to generate enough roll calendar entries to "overlap" with the end of the existing multiple prices
    if len(repo_roll_calendar.index) > 1:
        last_roll_index = -2
    else: 
        last_roll_index = -1

    last_contract = min(repo_roll_calendar.iloc[last_roll_index].values)
    last_roll_date = repo_roll_calendar.index[last_roll_index]
    #print(last_roll_date)
    #print(last_roll_date.year, last_roll_date.month, last_roll_date.day)
    roll_date_month = int("{:04d}".format(last_roll_date.year)+"{:02d}".format(last_roll_date.month)+"00")
    last_contract = min(last_contract, roll_date_month)
    
    parquet_path = config.get_element("parquet_store")+'/'+CONTRACT_COLLECTION+'/'
    instrument_parquet_files = glob(parquet_path+instrument+"#*"+".parquet")
    contract_list = [os.path.basename(file).replace('.parquet', '').split('#')[1] for file in instrument_parquet_files]
    contracts_to_copy = [contract for contract in contract_list if contract >= str(last_contract)]

    contract_files_to_copy = [parquet_path+instrument+"#"+contract+ ".parquet" for contract in contracts_to_copy]
    for file in contract_files_to_copy:
        os.system('cp '+file+' '+tmp_futures_contract_price_parquets_contract_collection)


def correct_generated_roll_calendars(instrument_code, system_roll_calendars_path, generated_roll_calendars_path, patched_roll_calendars_path):
    # Run this function after generating a temporary roll calendar and before generating the temporary multiple prices. 
    # This function assumes that the system roll calendar has a spurious entry which only reflects the last available data point, as the roll calendar was likely generated from the multiple prices
    # The generated roll calendar will also have a spurious entry, reflecting the availability of the contract price data
    # This function identifies the "roll" in the generated calendar that is the same "roll" as the last of the system calendar. There are two possibilities: 
    #   1. The generated calendar has a later datetime, which should be the more common and expected case. 
    #   2. The generated calendar has an earlier datetime, this should be less common.
    # For case 1: 
    #   The last line of the system roll calendahong r should be discarded, and the generated roll calendar should be then patched over to the system calendar. 
    #   In addition, to generate the multiple prices needed to be spliced to the existing multiple prices file, we need to patch the generated roll calendar with the 2nd last line of the existing calendar. 
    # For case 2: 
    #   The first line of the generated roll calendar should be discarded, and the system roll calendar should be kept as is. 
    #   The generated calendar is then patched over the system roll calendar. 
    #   Also copy over the last line of the existing roll caledar to the generated calendar to ensure the right multiple prices are generated. 
    system_roll_calendar_file = os.path.join(system_roll_calendars_path, instrument_code + '.csv')
    generated_roll_calendar_file = os.path.join(generated_roll_calendars_path, instrument_code + '.csv')
    patched_roll_calendar_file = os.path.join(patched_roll_calendars_path, instrument_code + '.csv')

    if not os.path.exists(system_roll_calendar_file):
        print("There is no existing roll calendar for", instrument_code)
        return False
    if not os.path.exists(generated_roll_calendar_file):    
        print("There is no generated roll calendar for", instrument_code)
        return False
    
    system_roll_calendar = pd.read_csv(system_roll_calendar_file, index_col=0, parse_dates=True)
    generated_roll_calendar = pd.read_csv(generated_roll_calendar_file, index_col=0, parse_dates=True)

    #find the row in the generated roll calendar that corresponds to the 'next roll', in the last line of the system roll calendar
    #print(system_roll_calendar.tail(5))
    #print(generated_roll_calendar.head(5))
    while True:
        if system_roll_calendar.iloc[-1].equals(generated_roll_calendar.iloc[0]):
        #print("The last line of the system roll calendar is the same as the first line of the generated roll calendar for", instrument_code)
            break; 
        else:
            generated_roll_calendar.drop(generated_roll_calendar.index[0], inplace=True)  # drop the first line of the generated roll calendar
            if len(generated_roll_calendar.index) == 0:
                print("Cannot find the last roll from the system roll calendar in generated data. Something is wrong  for", instrument_code)
                return False
            

    #compare the datetime of the 'next roll' from the system roll calendar (the last line, usually generated and not true) and the 'next roll' from the generated roll calendar (the first or 2nd line, usually true)
    system_roll_datetime = system_roll_calendar.index[-1]
    generated_roll_datetime = generated_roll_calendar.index[0]
    
    if generated_roll_datetime > system_roll_datetime:
        # scratch the last line of the system roll calendar 
        # copy over the 2nd last line of the system roll calendar to the generated roll calendar (the last 'actual' roll)
        # patch the system roll calendar with the generated roll calendar
        print("The generated roll calendar has a later datetime than the system roll calendar for", instrument_code)
        system_roll_calendar.drop(system_roll_calendar.index[-1], inplace=True)  # drop the last line of the system roll calendar
        last_actual_roll = system_roll_calendar.tail(1) # get the last line of the system roll calendar, which is the last 'actual' roll
        #last_actual_roll = system_roll_calendar.iloc[-1] # get the 2nd last line of the system roll calendar
    else: 
        # drop the first roll generated,  which will be replaced by the last line from the system roll calendar (dictated by the actual multiple prices)
        print("The generated roll calendar has an earlier datetime than the system roll calendar for", instrument_code)
        generated_roll_calendar.drop(generated_roll_calendar.index[0], inplace=True)
        last_actual_roll = system_roll_calendar.tail(2) # get the last two line of the system roll calendar, the last 'actual' roll and the last 'generated' roll which may be an artifact of the multiple prices

    
    system_roll_calendar = pd.concat([system_roll_calendar, generated_roll_calendar], axis=0)
    #append the last actual roll to the generated roll calendar
    generated_roll_calendar = pd.concat([last_actual_roll, generated_roll_calendar])
    generated_roll_calendar.to_csv(generated_roll_calendar_file)
    system_roll_calendar.to_csv(patched_roll_calendar_file)
    #print(system_roll_calendar.head(2))
    #print(generated_roll_calendar.head(2) )


                
def generate_spliced_multiple_prices(instrument_code, multiple_prices_from_db, spliced_multiple_prices):
    supplied_file = os.path.join('data', 'futures', 'multiple_prices_csv', instrument_code + '.csv') # repo data
    generated_file = os.path.join(multiple_prices_from_db, instrument_code + '.csv')
    supplied = pd.read_csv(supplied_file, index_col=0, parse_dates=True)
    generated = pd.read_csv(generated_file, index_col=0, parse_dates=True)

    columns_to_order = supplied.columns.tolist()
    generated = generated[columns_to_order]  # ensure the columns are in the same order as supplied
    generated.to_csv(generated_file)  # save the generated file after sorting the columns 

    # get final datetime of the supplied multiple_prices for this instrument
    last_supplied = supplied.index[-1] 
    #print(f"last datetime of supplied prices {last_supplied}, first datetime of updated prices is {generated.index[0]}")

    #re-writing Carver's code here: 
    #1. The reference of slicing using generated.loc[last_supplied:] repeatedly is clearly inefficient and redundant
    #2. More importantly, in the original code, the overlapping line, as indicated by generated.loc[last_supplied:].iloc[0] is discarded before the price and forward comparison. This would become a problem if this line happens to be a roll
    #3. Compare the code below to the code in the repo document to see the difference 

    # assuming the latter is later than the former, truncate the generated data:
    generated = generated.loc[last_supplied:]
    # check we're using the same price and forward contracts (i.e. no rolls missing, which there shouldn't be if there is date overlap)
    assert(supplied.iloc[-1].PRICE_CONTRACT == generated.iloc[0].PRICE_CONTRACT)
    assert(supplied.iloc[-1].FORWARD_CONTRACT == generated.iloc[0].FORWARD_CONTRACT)

    # if first datetime in generated is the same as last datetime in repo, skip that row
    first_generated = generated.index[0] 
    overlapped_row = generated.iloc[[0]]
    if first_generated == last_supplied:
        generated = generated.iloc[1:]

    # nb we don't assert that the CARRY_CONTRACT is the same for supplied and generated, as some of the rolls implicit in the supplied multiple_prices don't match the pattern in the rollconfig.csv
    spliced = pd.concat([supplied, generated])
    #If the supplied data contains NAs in the overlapping row that has value in the generated data, update the NAs with data
    spliced = spliced.combine_first(overlapped_row) 
    spliced.to_csv(os.path.join(spliced_multiple_prices, instrument_code+'.csv'))

    #from sysinit.futures.multiple_and_adjusted_from_csv_to_db import init_db_with_csv_prices_for_code
    #init_db_with_csv_prices_for_code(instrument_code, multiple_price_datapath=spliced_multiple_prices)


def update_repo_multiple_prices_for_instrument (instrument_code): 
    #1. generate a roll calendar from existing multiple prices, 
    #2. copy the futures contract parquets 
    #3. generate the roll calendar from the copied parquet files
    #4. generate multiple prices from the generated roll calendar 
    #5. Check if the generated multiple prices have the same last line as the existing multiple prices, if not there is likely a spurious roll in the generated roll calendar
    #6. Deal with the spurious roll problem 
    config = Config()
    config = get_production_config()
    roll_calendars_from_multiple_prices = os.path.join('data', 'futures', 'roll_calendars_from_multiple_prices')
    roll_calendars_from_db = os.path.join('data', 'futures', 'roll_calendars_from_db')
    multiple_prices_from_db = os.path.join('data', 'futures', 'multiple_from_db')
    spliced_multiple_prices = os.path.join('data', 'futures', 'multiple_prices_csv_spliced')
    patched_roll_calendars = os.path.join('data', 'futures', 'patched_roll_calendars')
    tmp_futures_contract_price_parquets = os.path.join(config.get_element("parquet_store")+'/'+'tmp_futures_contract_price_parquets')
    tmp_futures_contract_price_parquets_contract_collection = tmp_futures_contract_price_parquets + '/' + CONTRACT_COLLECTION

    auxiliary_folders = [roll_calendars_from_multiple_prices, roll_calendars_from_db, multiple_prices_from_db, spliced_multiple_prices, patched_roll_calendars, tmp_futures_contract_price_parquets, tmp_futures_contract_price_parquets_contract_collection]
    for folder in auxiliary_folders:
        if not os.path.exists(folder):
            os.makedirs(folder)

    #1. generate roll calendar from existing multiple prices
    diag_prices = diagPrices()
    csv_roll_calendars_from_mulitple_prices = csvRollCalendarData(roll_calendars_from_multiple_prices)
    db_multiple_prices = diag_prices.db_futures_multiple_prices_data
    multiple_prices = db_multiple_prices.get_multiple_prices(instrument_code)
    roll_calendar_from_multiple_prices = rollCalendar.back_out_from_multiple_prices(multiple_prices)
    #note that the backed out roll calendar from the multiple prices will have a spurious last line so we'll remove it. 
    #Removing the spurious last line also ensures that enough futures_contract_prices_parquets are copied over to the temporary folder for the next steps 
    roll_calendar_from_multiple_prices = remove_spurious_roll_from_roll_calendar_data(roll_calendar_from_multiple_prices)
    csv_roll_calendars_from_mulitple_prices.add_roll_calendar(instrument_code, roll_calendar_from_multiple_prices, ignore_duplication=True)

    #2. copy contract parquets to the temporary folder for the instrument. 
    copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument_code, config, csv_roll_calendars_from_mulitple_prices,tmp_futures_contract_price_parquets_contract_collection)

    #3. generate roll calendar from the copied parquet files
    tmp_futures_contract_parquet_access = ParquetAccess(tmp_futures_contract_price_parquets)
    tmp_parquet_futures_contract_price_data = parquetFuturesContractPriceData(tmp_futures_contract_parquet_access)
    generated_roll_calendar = build_and_write_roll_calendar(instrument_code,input_prices=tmp_parquet_futures_contract_price_data, output_datapath=roll_calendars_from_db,check_before_writing=False)

    #3.1 prune the last line of the generated roll calendar
    generated_roll_calendar = remove_spurious_roll_from_roll_calendar_data(generated_roll_calendar)
    csv_roll_calendars_from_db = csvRollCalendarData(roll_calendars_from_db)
    csv_roll_calendars_from_db.add_roll_calendar(instrument_code, generated_roll_calendar, ignore_duplication=True)

    #4. generate multiple prices from the generated roll calendar
    generated_multiple_prices = process_multiple_prices_single_instrument(instrument_code, csv_multiple_data_path=multiple_prices_from_db,  ADD_TO_DB=False, csv_roll_data_path=roll_calendars_from_db, ADD_TO_CSV=True)

    #5. check if the last line of existing multiple prices is in the generated multiple prices, if not there is an issue
    last_supplied_multiple_prices_dt = multiple_prices.index[-1]
    generated = futuresMultiplePrices(generated_multiple_prices.loc[last_supplied_multiple_prices_dt:])

    #5.1 While it is generally useful to remove the last line of the generated roll calendar to avoid spurious rolls, it is possible that the last line is an acutal roll
    #Removing an actual roll would keep a contract for too long. As a remedy, we remove the multiple prices rows from the last row, for all rows without a PRICE for PRICE_CONTRACT
    #generated = generated[isnull(generated.PRICE & ~generated.PRICE.isna]
    while not generated.empty: 
        if generated.iloc[-1].PRICE is not None and not pd.isna(generated.iloc[-1].PRICE):
            break
        else:
            generated = generated.iloc[:-1]

    if generated.empty:
        print(f"Generated multiple prices for {instrument_code} is empty after removing rows without PRICE, skipping splicing")
        return None
    
    first_generated = generated.index[0]
    if first_generated == last_supplied_multiple_prices_dt:
        # check we're using the same price and forward contracts (i.e. no rolls missing, which there shouldn't be if there is date overlap)
        # nb we don't assert that the CARRY_CONTRACT is the same for supplied and generated, as some of the rolls implicit in the supplied multiple_prices don't match the pattern in the rollconfig.csv
        assert(str(multiple_prices.iloc[-1].PRICE_CONTRACT) == str(generated.iloc[0].PRICE_CONTRACT))  #the typecast is necessary as the comparison sometimes fails with undefined datatype of PRICE_CONTRACT and FORWARD_CONTRACT
        assert(str(multiple_prices.iloc[-1].FORWARD_CONTRACT) == str(generated.iloc[0].FORWARD_CONTRACT))
        overlapped_row = generated.iloc[[0]]
        generated = generated.iloc[1:]
        spliced = pd.concat([multiple_prices, generated])
        #If the supplied data contains NAs in the overlapping row that has value in the generated data, update the NAs with data
        spliced = spliced.combine_first(overlapped_row)  
        #spliced.to_csv(os.path.join(spliced_multiple_prices, instrument_code+'.csv'))
        spliced_multiple_prices = csvFuturesMultiplePricesData(spliced_multiple_prices)
        spliced_multiple_prices.add_multiple_prices(instrument_code, spliced, ignore_duplication=True)
        return spliced
    else:
        #in this case the generated multiple prices has no overlap with the existing multiple prices 
        #The way pandas dataframe is sliced means that the first row of the generated multiple prices is at a later datetime than the last row of the existing multiple prices
        #The price and forward contracts should be no later than the contracts in the last row of the existing multiple prices
        assert(str(multiple_prices.iloc[-1].PRICE_CONTRACT) <= str(generated.iloc[0].PRICE_CONTRACT))  
        assert(str(multiple_prices.iloc[-1].FORWARD_CONTRACT) <= str(generated.iloc[0].FORWARD_CONTRACT))
        spliced = pd.concat([multiple_prices, generated])
        spliced_multiple_prices = csvFuturesMultiplePricesData(spliced_multiple_prices)
        spliced_multiple_prices.add_multiple_prices(instrument_code, spliced, ignore_duplication=True)
        return spliced

#Update repo multiple prices for all instruments: 
if __name__ == "__main__":

    #Backup existing system roll calendars
    backup_repo_data()
    
    db_multiple_prices_parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
    db_multiple_prices = parquetFuturesMultiplePricesData(db_multiple_prices_parquet_access)
    multiple_prices_instrument_list = db_multiple_prices.get_list_of_instruments()
    print(multiple_prices_instrument_list)
    
    instrument_pickle_file = 'processed_tickers.pkl'
    if os.path.exists(instrument_pickle_file):
        with open(instrument_pickle_file,'rb') as file:  
            processed_instruments = pickle.load(file)
    else:
        processed_instruments = []
    processed_instruments = []

    for instrument in multiple_prices_instrument_list:
    #for instrument in ['MILKDRY']:
        if instrument in processed_instruments: 
            continue

        print(instrument)
        try:
            update_repo_multiple_prices_for_instrument(instrument)
        except Exception as e:
            print(f"Error processing {instrument}: {e}")
            continue

        processed_instruments += [instrument]
        with open (instrument_pickle_file, 'wb') as file: 
            pickle.dump(processed_instruments, file)

###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
###############################################################################################################################################################################################################
#TEST CODE THAT IS NOT USED IN THE ACTUAL SCRIPT, JUST FOR CHECKING AND DIAGNOSIS PURPOSES.

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


#print(tmp_parquet_futures_contract_price_data)
#tmp_dict_of_all_futures_contract_prices = tmp_parquet_futures_contract_price_data.get_merged_prices_for_instrument( instrument_code )
#tmp_dict_of_futures_contract_prices = tmp_dict_of_all_futures_contract_prices.final_prices()
#print(tmp_dict_of_all_futures_contract_prices)
    

##THis is too much of special case and should not be used. Need a more robust approach to handle spurious rolls. 
def generate_spliced_multiple_prices_with_spurious_roll(instrument_code, multiple_prices_from_db, spliced_multiple_prices):
    # Similar to the previous function, but with additional handling for spurious rolls
    # 2025.08.17  see Notion notes on this issue https://www.notion.so/Multiple-Prices-from-spurious-rolls-25239604e82e8046b0add26bc508ebf3?source=copy_link
    supplied_file = os.path.join('data', 'futures', 'multiple_prices_csv', instrument_code + '.csv') # repo data
    generated_file = os.path.join(multiple_prices_from_db, instrument_code + '.csv')

    supplied = pd.read_csv(supplied_file, index_col=0, parse_dates=True)
    generated = pd.read_csv(generated_file, index_col=0, parse_dates=True)

    spliced_file = os.path.join(spliced_multiple_prices, instrument_code+'.csv')
    #if spliced_file exists and is older than the generated_file , then we'd have to deal with spurious rows otherwise we can skip
    if os.path.exists(spliced_file) and os.path.getmtime(spliced_file) < os.path.getmtime(generated_file):
        ...
    else:
        return
    print('here')
        
    while True:
        last_supplied_dt = supplied.index[-1]

        #find last_supplied_dt in generated.index
        last_supplied_loc = generated.index.get_loc(last_supplied_dt) 
        if last_supplied_loc is None:
            print(f"last_supplied_dt for {instrument_code} is not found in genereated multiple prices, skipping splicing")
            return
        
        if supplied.iloc[-1].PRICE_CONTRACT == generated.iloc[last_supplied_loc].PRICE_CONTRACT:
            #found the overlapping dt, created spliced multiple prices 
            generated_for_spliced = generated.iloc[last_supplied_loc:]
            overlapped_row = generated_for_spliced.iloc[[0]]

            if len(generated_for_spliced) >1 :
                generated_for_spliced = generated_for_spliced.iloc[1:]
                spliced = pd.concat([supplied, generated_for_spliced])
            else: 
                spliced = supplied
                
            #If the supplied data contains NAs in the overlapping row that has value in the generated data, update the NAs with data
            spliced = spliced.combine_first(overlapped_row)
            spliced.to_csv(os.path.join(spliced_multiple_prices, 'tmp', instrument_code+'.csv'))
            return
        else:
            supplied.drop(last_supplied_dt, inplace=True)  # drop the last row of supplied

        #if supplied is now an empty dataframe then something is wrong
        if supplied.empty:
            print(f"supplied is empty after dropping last_supplied_dt for {instrument_code}, skipping splicing")
            return
