#This code removes the IB futures data with zero volumes for barSizes of 5 mins, 15 mins, and 1 hour
#It can be adapted for other bar sizes as needed, or other data set

from mtfuturesdata.mtIBData import remove_zero_volumes
import pandas as pd
import os
from mttestscripts.files_tool import list_all_instruments_from_a_directory

def remove_zero_volumes_from_one_parquet(input_parquet, output_parquet):
    df = pd.read_parquet(input_parquet)
    df = remove_zero_volumes(df, 'volume')
    if (df is None) or df.empty:
        ...
    else:
        df.to_parquet(output_parquet)
    return df


def remove_zero_volumes_from_all_ib_parquets():
    folders = ['CTH_5_mins', 'CTH_15_mins', 'CTH_1_hour']
    source_root = '/mnt/sda1/data/parquet/ib'
    target_root = '/mnt/sda1/tmp'

    def create_folders():
        for folder in folders:
            os.makedirs(os.path.join(target_root, folder), exist_ok=True)

    create_folders()
    for folder in folders:
        input_parquets = list_all_instruments_from_a_directory(os.path.join(source_root, folder), extension='.parquet')
        for input_parquet in input_parquets:
            output_parquet = os.path.join(target_root, folder, os.path.basename(input_parquet))
            remove_zero_volumes_from_one_parquet(os.path.join(source_root, folder, os.path.basename(input_parquet)), output_parquet)

if __name__ == "__main__":
    remove_zero_volumes_from_all_ib_parquets()