#Export a parquet file to a CSV file for inspection.

import pandas as pd
import os
from glob import glob

def convert_parquet_to_csv(parquet_file_path, csv_file_path):
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file_path)
    
    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=True)

#This is for checking CLP contract prices parquets. 
#As it turned out, BarChart and PST downloaded different contracts which resulted in spikes when updating historical data (from BarChart) with latest data (from IB)
def check_clp_files():
    clp_parquet_file_path = "/mnt/sda1/data/parquet/futures_contract_prices/"
    csv_output_file_path = '/mnt/sda1/data/tmp/'

    all_clp_files = glob(clp_parquet_file_path+"Day@CLP#*.parquet")
    print(all_clp_files)
    [convert_parquet_to_csv(file, csv_output_file_path + os.path.basename(file).replace('.parquet', '.csv')) for file in all_clp_files]

if __name__ == "__main__":
    check_clp_files()

