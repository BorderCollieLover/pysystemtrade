# Used in the process of updating roll calendars and multiple prices in bulk processes.
# Compare two directories and see which instruments are missing in the steps of generating updates for roll calendars and multiple prices.
import os
import pandas as pd
from glob import glob
from sysdata.config.production_config import get_production_config, Config
from sysdata.data_blob import dataBlob
from sysproduction.update_multiple_adjusted_prices import calc_update_adjusted_prices
from sysproduction.data.prices import (
    diagPrices,
    updatePrices,
    get_valid_instrument_code_from_user,
)


def is_file1_newer(file1_path, file2_path):
    #print(file1_path)
    if not os.path.exists(file1_path):
        #print('here')
        return False  # Handle cases where one or both files don't exist
    
    if not os.path.exists(file2_path):
        #print('here2')
        return True 

    mod_time_file1 = os.path.getmtime(file1_path)
    mod_time_file2 = os.path.getmtime(file2_path)
    #print(mod_time_file1, mod_time_file1)

    return mod_time_file1 > mod_time_file2

def convert_parquet_to_csv(parquet_file_path, csv_file_path, update_only=True):
    # Read the Parquet file into a DataFrame

    #print(parquet_file_path)
    try:
        df = pd.read_parquet(parquet_file_path)
    except Exception as e: 
        print(e, parquet_file_path)
        return
   
    #df.to_csv(csv_file_path, index=True)
    # Write the DataFrame to a CSV file
    if update_only:
        if is_file1_newer(parquet_file_path, csv_file_path):
            #print('True')
            df.to_csv(csv_file_path, index=True)
            return
        else:
            #verify that the csv has as many lines of data
            try:
                df2 = pd.read_csv(csv_file_path, header=0)
            except Exception as e: 
                print(e)
                df.to_csv(csv_file_path, index=True)
                return

            if df2.empty: 
                df.to_csv(csv_file_path, index=True)
                return
            
            if not df2.empty: 
                if len(df2) < len(df):
                    df.to_csv(csv_file_path, index=True)
    else:
        df.to_csv(csv_file_path, index=True)

def ib_ohlcv_csv_to_parquet(csv_file, parquet_file):
    data = pd.read_csv(csv_file, index_col='date')
    print(len(data))
    data.to_parquet(parquet_file)


def list_all_instruments_from_a_directory(directory,  extension='.csv'):
    """
    List all instrument files in a directory with a specific extension.
    """
    return [f for f in os.listdir(directory) if f.endswith(extension)]


def find_missing_instruments(dir1, dir2):
    """
    Find missing instruments between two directories.
    """
    instruments_dir1 = set(list_all_instruments_from_a_directory(dir1))
    instruments_dir2 = set(list_all_instruments_from_a_directory(dir2))
    return instruments_dir1 - instruments_dir2


def backup_one_folder(source_folder, backup_folder, extension='.csv', update_only=True):
    # This function is a placeholder for the backup of existing system roll calendars
    # It should create a backup of the existing roll calendars before updating them
    # The backup can be done by copying the existing roll calendars to a backup folder

    if not os.path.exists(backup_folder):   
        os.makedirs(backup_folder)
    if os.path.exists(source_folder):
        for file in os.listdir(source_folder):
            if file.endswith(extension):
                src_file = os.path.join(source_folder, file)
                dst_file = os.path.join(backup_folder, file)
                if update_only: 
                    if is_file1_newer(src_file, dst_file):
                        os.system(f'cp {src_file} {dst_file}')
                else:
                    os.system(f'cp {src_file} {dst_file}')


def replace_file_extension(filename, new_extension):
    root, old_extension = os.path.splitext(filename)
    new_filename = root + new_extension
    return (new_filename)

def back_up_parquet_into_csv(source_folder, backup_folder):
    if not os.path.exists(backup_folder):   
        os.makedirs(backup_folder)
    if os.path.exists(source_folder):
        for item in os.listdir(source_folder):
            if item.endswith('.parquet'):
                #print(item)
                src_file = os.path.join(source_folder, item)
                tgt_file = os.path.join(backup_folder, replace_file_extension(item, '.csv'))
                #print(src_file, tgt_file)
                convert_parquet_to_csv(src_file, tgt_file)
            else: # the iten is a folder
                new_source_folder = os.path.join(source_folder, item)
                if os.path.isdir(new_source_folder):
                    new_backup_folder = os.path.join(backup_folder, item)
                    print(new_source_folder, new_backup_folder)
                    back_up_parquet_into_csv(new_source_folder, new_backup_folder)
                    

def fix_str_index_to_datetime(parquet_name, tzinfo='UTC'):
    ...
    #if the index of a time series parquet is not recognized as datetime 
    # try to convert it using pd.to_datetime() to fix the issue 




if __name__ == "__main__":
    """ system_roll_calendars_path = os.path.join('data', 'futures', 'roll_calendars_csv')
    generated_roll_calendars_path = os.path.join('data', 'futures', 'roll_calendars_from_db')

    missing_generated_roll_calendars = find_missing_instruments(system_roll_calendars_path, generated_roll_calendars_path)
    print("Missing generated roll calendars:", missing_generated_roll_calendars)

    generated_multiple_prices_path = os.path.join('data', 'futures', 'multiple_from_db')
    missing_multiple_prices = find_missing_instruments(generated_roll_calendars_path, generated_multiple_prices_path   )
    print("Missing generated multiple prices:", missing_multiple_prices)

    spliced_multiple_prices_path = os.path.join('data', 'futures', 'multiple_prices_csv_spliced')
    missing_spliced_multiple_prices = find_missing_instruments(generated_multiple_prices_path, spliced_multiple_prices_path)
    print("Missing spliced multiple prices:", sorted(missing_spliced_multiple_prices)) """

    #back_up_parquet_into_csv('/mnt/sda1/data/parquet/futures_adjusted_prices/','/mnt/sda1/data/parquet/CSV/futures_adjusted_prices/' )

    file_name = 'Hour@SARONA#20230900'
    file_name = '1B05H_691184271'
    folder = 'futures_contract_prices'
    folder = 'ib/CTH_5_mins'

    config = Config()
    config = get_production_config()
    source_file_path = os.path.join(config.get_element("parquet_store"), folder)
    src_parquet = os.path.join(source_file_path, file_name+'.parquet')
    #convert_parquet_to_csv(src_parquet, 'foo.csv')

    #filename = '/mnt/sda1/data/parquet/ib/RTH_1_day/UINK5_772076870.parquet'
    #convert_parquet_to_csv(filename, '/mnt/sda1/foo.csv')

    #csv_file = '/mnt/sda1/data/DATABackup/ib/RTH_1_day/ECOG8_803750278.csv'
    #parquet_file = '/mnt/sda1/data/parquet/ib/RTH_1_day/ECOG8_803750278.parquet'
    #ib_ohlcv_csv_to_parquet(csv_file, parquet_file)



    #csv_file = '/mnt/sda1/data/DATABackup/ib/RTH_1_day/FDIV  25L19_461926858.csv'
    #parquet_file = '/mnt/sda1/data/parquet/ib/RTH_1_day/FDIV  25L19_461926858.parquet'
    #ib_ohlcv_csv_to_parquet(csv_file, parquet_file)
