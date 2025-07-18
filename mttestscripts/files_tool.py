# Used in the process of updating roll calendars and multiple prices in bulk processes.
# Compare two directories and see which instruments are missing in the steps of generating updates for roll calendars and multiple prices.

import os
import pandas as pd
from glob import glob

def convert_parquet_to_csv(parquet_file_path, csv_file_path):
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file_path)
    
    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=True)


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


def backup_one_folder(source_folder, backup_folder, extension='.csv'):
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
                os.system(f'cp {src_file} {dst_file}')

if __name__ == "__main__":
    system_roll_calendars_path = os.path.join('data', 'futures', 'roll_calendars_csv')
    generated_roll_calendars_path = os.path.join('data', 'futures', 'roll_calendars_from_db')

    missing_generated_roll_calendars = find_missing_instruments(system_roll_calendars_path, generated_roll_calendars_path)
    print("Missing generated roll calendars:", missing_generated_roll_calendars)

    generated_multiple_prices_path = os.path.join('data', 'futures', 'multiple_from_db')
    missing_multiple_prices = find_missing_instruments(generated_roll_calendars_path, generated_multiple_prices_path   )
    print("Missing generated multiple prices:", missing_multiple_prices)

    spliced_multiple_prices_path = os.path.join('data', 'futures', 'multiple_prices_csv_spliced')
    missing_spliced_multiple_prices = find_missing_instruments(generated_multiple_prices_path, spliced_multiple_prices_path)
    print("Missing spliced multiple prices:", sorted(missing_spliced_multiple_prices))