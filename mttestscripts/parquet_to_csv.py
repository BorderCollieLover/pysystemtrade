#Export a parquet file to a CSV file for inspection.
import os
from glob import glob
from mttestscripts.files_tool import convert_parquet_to_csv

#This is for checking CLP contract prices parquets. 
#As it turned out, BarChart and PST downloaded different contracts which resulted in spikes when updating historical data (from BarChart) with latest data (from IB)
def check_clp_files():
    clp_parquet_file_path = "/mnt/sda1/data/parquet/futures_contract_prices/"
    csv_output_file_path = '/mnt/sda1/data/tmp/'

    all_clp_files = glob(clp_parquet_file_path+"Day@CLP#*.parquet")
    print(all_clp_files)
    [convert_parquet_to_csv(file, csv_output_file_path + os.path.basename(file).replace('.parquet', '.csv')) for file in all_clp_files]

if __name__ == "__main__":
    #check_clp_files()
    parquet_path = '/mnt/sda1/data/parquet/tmp_futures_contract_price_parquets/futures_contract_prices/'
    #parquet_path = '/mnt/sda1/data/parquet/futures_multiple_prices/'
    csv_output_path = '/mnt/sda1/data/tmp/'

    file_name = 'VNKI#20240400.parquet'
    convert_parquet_to_csv(os.path.join(parquet_path, file_name), os.path.join(csv_output_path, file_name.replace('.parquet', '.csv')))

