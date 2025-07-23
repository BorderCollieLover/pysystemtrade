import os
#from turtle import pd
import pandas as pd
from sysinit.futures.rollcalendars_from_db_prices_to_csv import build_and_write_roll_calendar, check_saved_roll_calendar
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from sysdata.config.production_config import get_production_config, Config
from glob import glob 
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData
from sysdata.parquet.parquet_access import ParquetAccess
from sysproduction.data.prices import diagPrices
from sysinit.futures.multipleprices_from_db_prices_and_csv_calendars_to_db import process_multiple_prices_single_instrument
from mttestscripts.files_tool import list_all_instruments_from_a_directory, backup_one_folder
import pickle 



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
    
    patched_roll_calendars = os.path.join('data', 'futures', 'patched_roll_calendars')
    if not os.path.exists(patched_roll_calendars):
        os.makedirs(patched_roll_calendars)
    return(roll_calendars_from_db, multiple_prices_from_db, spliced_multiple_prices, patched_roll_calendars)

def prepare_adjust_prices_csv_for_update():
    #This function is used to prepare the adjusted prices csv files for updating
    #The main purpose is to ensure that the adjusted prices and multiple prices are in sync with each other


    multiple_prices_csv = os.path.join('data', 'futures', 'multiple_prices_csv')
    adjusted_prices_csv = os.path.join('data', 'futures', 'adjusted_prices_csv')

    assert os.path.exists(multiple_prices_csv), "The multiple prices CSV directory does not exist."
    assert os.path.exists(adjusted_prices_csv), "The adjusted prices CSV directory does not exist."

    instruments = list_all_instruments_from_a_directory(adjusted_prices_csv, extension='.csv')
    if not instruments: 
        print("No instruments found in the adjusted prices CSV directory.")
        return
    
    for instrument in instruments:
        multiple_prices_file = os.path.join(multiple_prices_csv, instrument)
        if not os.path.exists(multiple_prices_file):
            print(f"Multiple prices CSV for {instrument} does not exist. Skipping.")
            continue

        adjusted_prices_file = os.path.join(adjusted_prices_csv, instrument)

        multiple_prices = pd.read_csv(multiple_prices_file, index_col=0, parse_dates=True)
        adjusted_prices = pd.read_csv(adjusted_prices_file, index_col=0, parse_dates=True)

        last_adjusted_date = adjusted_prices.index[-1] if not adjusted_prices.empty else None
        last_multiple_date = multiple_prices.index[-1] if not multiple_prices.empty else None
        if last_adjusted_date is None or last_multiple_date is None:
            print(f"One of the files for {instrument} is empty. Skipping.")
            continue

        if last_adjusted_date < last_multiple_date:
            # If the last adjusted date is earlier than the last multiple prices date, we need to adjust the prices CSV
            print(f"Adjusting prices for {instrument}. Last adjusted date: {last_adjusted_date}, Last multiple prices date: {last_multiple_date}")
        else: 
            if last_adjusted_date > last_multiple_date:
            # If the last adjusted date is later than the last multiple prices date, we need to adjust the multiple prices CSV
                print(f"Adjusting multiple prices for {instrument}. Last adjusted date: {last_adjusted_date}, Last multiple prices date: {last_multiple_date}")
        
        

    return

def backup_repo_data():
    repo_paths = ['roll_calendars_csv', 'multiple_prices_csv', 'adjusted_prices_csv']

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
    #for instrument in ['LEAD_LME', 'TIN_LME', 'ZINC_LME']:
        print(instrument)
        copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument, config, repo_roll_calendar_data,tmp_futures_contract_price_parquets_contract_collection)
    return

#For each instrument, find the last line in current roll calendar
#From this line, find the earliest contract used in this roll calendar, use this as the reference contract
#From the PST system futures contract prices repository (parquet_store  + CONTRACT_COLLECTION)
#Find all merged price parquet objects of this instrument, copy contracts that are no older than the reference contract to the temporary folder created above
#However, since the roll calendar's last line is usually a spurious entry, we actually go back one more line in the roll calendar
def copy_futures_contract_price_parquets_for_roll_calendar_for_instrument(instrument, config, repo_roll_calendar_data,tmp_futures_contract_price_parquets_contract_collection):
    repo_roll_calendar = repo_roll_calendar_data.get_roll_calendar(instrument)
    #end_date = repo_roll_calendar.index.max()
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

def correct_generated_roll_calendars(instrument_code, system_roll_calendars_path, generated_roll_calendars_path, patched_roll_calendars_path):
    # Run this function after generating a temporary roll calendar and before generating the temporary multiple prices. 
    # This function assumes that the system roll calendar has a spurious entry which only reflects the last available data point, as the roll calendar was likely generated from the multiple prices
    # The generated roll calendar will also have a spurious entry, reflecting the availability of the contract price data
    # This function identifies the "roll" in the generated calendar that is the same "roll" as the last of the system calendar. There are two possibilities: 
    #   1. The generated calendar has a later datetime, which should be the more common and expected case. 
    #   2. The generated calendar has an earlier datetime, this should be less common.
    # For case 1: 
    #   The last line of the system roll calendar should be discarded, and the generated roll calendar should be then patched over to the system calendar. 
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

    import pandas as pd
    supplied = pd.read_csv(supplied_file, index_col=0, parse_dates=True)
    generated = pd.read_csv(generated_file, index_col=0, parse_dates=True)

    columns_to_order = supplied.columns.tolist()
    generated = generated[columns_to_order]  # ensure the columns are in the same order as supplied
    generated.to_csv(generated_file)  # save the generated file after sorting the columns 

    # get final datetime of the supplied multiple_prices for this instrument
    last_supplied = supplied.index[-1] 
    print(f"last datetime of supplied prices {last_supplied}, first datetime of updated prices is {generated.index[0]}")

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
    if first_generated == last_supplied:
        generated = generated.iloc[1:]

    # nb we don't assert that the CARRY_CONTRACT is the same for supplied and generated, as some of the rolls implicit in the supplied multiple_prices don't match the pattern in the rollconfig.csv
    spliced = pd.concat([supplied, generated])
    spliced.to_csv(os.path.join(spliced_multiple_prices, instrument_code+'.csv'))

    #from sysinit.futures.multiple_and_adjusted_from_csv_to_db import init_db_with_csv_prices_for_code
    #init_db_with_csv_prices_for_code(instrument_code, multiple_price_datapath=spliced_multiple_prices)

#Build all temporary roll calendars: 
if __name__ == "__main__":
    #Backup existing system roll calendars
    #backup_repo_data()

    roll_calendars_from_db, multiple_prices_from_db, spliced_multiple_prices, patched_roll_calendars = create_tmp_directories_for_update_roll_calendars()
    tmp_futures_contract_price_parquets, tmp_futures_contract_price_parquets_contract_collection = prepare_tmp_futures_contract_parquets_folder_for_updating_roll_calendars()
    #copy_futures_contract_price_parquets_for_roll_calendar(tmp_futures_contract_price_parquets_contract_collection)


    #This is where I create a tmp_parquet_futures_contract_price_data that points to the temporary directory where only futures contract prices since the last roll calendar line item is kept
    #The build_and_write_roll_calendar function will take the tmp_parquet_futures_contract_price_data object as an input. 
    #It will then generate roll calendars that can be patched to existing roll calendars 
    
    tmp_futures_contract_parquet_access = ParquetAccess(tmp_futures_contract_price_parquets)
    tmp_parquet_futures_contract_price_data = parquetFuturesContractPriceData(tmp_futures_contract_parquet_access)
    #print(tmp_parquet_futures_contract_price_data)
    #tmp_dict_of_all_futures_contract_prices = tmp_parquet_futures_contract_price_data.get_merged_prices_for_instrument( instrument_code )
    #tmp_dict_of_futures_contract_prices = tmp_dict_of_all_futures_contract_prices.final_prices()
    #print(tmp_dict_of_all_futures_contract_prices)
    system_roll_calendar_path = os.path.join('data', 'futures', 'roll_calendars_csv')
    repo_roll_calendar_data = csvRollCalendarData()
    
    #prepare_adjust_prices_csv_for_update()

    instrument_pickle_file = 'processed_tickers.pkl'
    if os.path.exists(instrument_pickle_file):
        with open(instrument_pickle_file,'rb') as file:  
            processed_instruments = pickle.load(file)
    else:
        processed_instruments = []

    for instrument in repo_roll_calendar_data.keys():
        if instrument in ['BB3M', 'BEL20', 'BRENT', 'COAL', 'EDOLLAR', 'ETHANOL', 'GAS-LAST', 'GAS-PEN', 'GAS_US_mini', 'HIGHYIELD', 'IG', 'IRON', 'LEAD_LME', 'MID-DAX', 'MILKWET', 'NIFTY-IN', 'NIFTY', 'OATIES', 'RICE', 'SARONA', 'SILVER-mini', 'SOFR', 'SONIA3', 'STEEL', 'TIN_LME', 'VIX_mini','VNKI', 'WHEY', 'ZINC_LME']:
            continue

        if instrument in processed_instruments: 
            continue

    #or instrument in ['LEAD_LME', 'TIN_LME', 'ZINC_LME']: #these tickers were patached when fixing historical data 
    #for instrument in ['SGX', 'VIX_mini']:
    #for instrument in ['BB3M', 'BEL20', 'BRENT', 'COAL', 'EDOLLAR', 'ETHANOL', 'GAS-LAST', 'GAS-PEN', 'GAS_US_mini', 'HIGHYIELD', 'IG', 'IRON', 'LEAD_LME', 'MID-DAX', 'MILKWET', 'NIFTY-IN', 'NIFTY', 'OATIES', 'RICE', 'SARONA', 'SILVER-mini', 'SOFR', 'SONIA3', 'STEEL', 'TIN_LME', 'VIX_mini', 'VNKI', 'WHEY', 'ZINC_LME']:
    #for instrument in ['BB3M']:
        print(instrument)
        #1. Generate a roll calendar for the instrument using the tmp_parquet_futures_contract_price_data
        try:
            ...
            #print(instrument)
            build_and_write_roll_calendar(instrument,input_prices=tmp_parquet_futures_contract_price_data, output_datapath=roll_calendars_from_db,check_before_writing=False)
        except Exception as e: 
            print(e)

        #2. Resolve the first roll in the generated roll calendar by comparing it to the last roll in the system roll calendar
        #   Patch up the system roll calendar with the correct generated roll calendar 
        #   Update the generated roll calendar with the 'real' last roll from the system roll calendar to ensure that the multiple prices generated from the roll calendar are correct without gaps
        correct_generated_roll_calendars(instrument, system_roll_calendar_path, roll_calendars_from_db,patched_roll_calendars)

        #3. Generate multiple prices for the instrument using the generated roll calendar
        try:
            ...
            process_multiple_prices_single_instrument(instrument, csv_multiple_data_path=multiple_prices_from_db,  ADD_TO_DB=False, csv_roll_data_path=roll_calendars_from_db, ADD_TO_CSV=True)
            generate_spliced_multiple_prices(instrument, multiple_prices_from_db, spliced_multiple_prices)
        except Exception as e:
            print(e)

        processed_instruments += [instrument]
        with open (instrument_pickle_file, 'wb') as file: 
            pickle.dump(processed_instruments, file)
