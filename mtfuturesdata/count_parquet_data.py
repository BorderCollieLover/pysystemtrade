import pandas as pd
from glob import glob
import datetime
import os

def count_one_parquet_file(parquet_obj):
    try:
        data = pd.read_parquet(parquet_obj)
        return(len(data))
    except Exception as e: 
        print(e)
        return(0)

def count_parquet_in_a_directory(file_path):
    #print(file_path)
    total_lines= 0 
    all_files = glob(file_path+"*.parquet")
    #print(len(all_files))
    if len(all_files)>0: 
        total_lines = sum([count_one_parquet_file(file) for file in all_files])
        print(f'Under %s: %d parquet objects with a total of %d data points, average %0.1f data points per contract.' %(file_path, len(all_files), total_lines, total_lines/len(all_files)))
    else:
        total_lines = 0
    
    return total_lines 

def count_empty_parquet_in_a_directory(file_path):
    #print(file_path)
    total_lines= 0 
    all_files = glob(file_path+"*.parquet")
    #print(len(all_files))
    parquet_lines = [count_one_parquet_file(file) for file in all_files]
    count = len([num for num in parquet_lines if num == 0])
    print(f'Under %s: %d parquet objects, %d of them are empty.' %(file_path, len(all_files), count))



def count_all():
    parquet_home = "/mnt/sda1/data/parquet/ib/"
    dirs = ['CTH_5_mins/', 'CTH_15_mins/', 'CTH_1_hour/', 'RTH_1_day/']
    total_num = sum([count_parquet_in_a_directory(parquet_home+dir) for dir in dirs])
    print('Total number of data points: {:,} .'.format(total_num))
    return(total_num)

def remove_empty_parquet(file_path):
    all_files = glob(file_path+"*.parquet")
    i = 0
    for file in all_files:
        if count_one_parquet_file(file) == 0:
            os.remove(file)
            #print(f'{file} is removed.')
            i = i + 1
    print(f'{i} empty parquet files are removed.')
    return None

if __name__ == "__main__":
    print(datetime.datetime.now())
    count_all()
    count_parquet_in_a_directory('/mnt/sda1/data/parquet/futures_contract_prices/')
    #count_empty_parquet_in_a_directory('/mnt/sda1/data/parquet/futures_contract_prices/')
    #remove_empty_parquet('/mnt/sda1/data/parquet/futures_contract_prices/')