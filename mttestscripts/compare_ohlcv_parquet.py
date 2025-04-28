#I updated my own data collection code on April 25, 2025. The update address two issues: 
# 1. When data retrieval process returns a data point with a 'future' timestamp. This happens when IB returns partial data reported for a contract for the current day, before the market closes. 
# 2. When combining newly retrieved data with existing data, the code now use the 'combine_first' method to resolve conflicts, whereas before, it keeps existing data. 
#I moved the downloaded data to the ib_historical folder, and started the data retrieval process again. I also flushed the meta data collections in MongoDB, so expired contracts will 
# have a chance to have their data retrieved again. I will run the updated data process code for a while and then I will merge the ib_historical data with the newly downloaded data. 

#This script compares the data in the ib_historical folder (data downloaded with the code before updates) with the data in the ib folder (newly downloaded data). 

import pandas as pd

#filename = 'CLM5_256019308'
#bar_setting_folder = 'RTH_1_day'
filename = 'RCK5_656391483'
bar_setting_folder = 'CTH_5_mins'


path_new = '/mnt/sda1/data/parquet/ib/'
path_old = '/mnt/sda1/data/parquet/ib_historical/' 
paths = [path_new, path_old]

def read_parquet_file(filename, bar_setting_folder, path):
    file_path = f"{path}{bar_setting_folder}/{filename}.parquet"
    data = pd.read_parquet(file_path)
    return data

def summarize_data(data):
    # Summarize the data
    summary = {
        'num_rows': len(data),
        'num_columns': len(data.columns),
        'columns': list(data.columns),
        'head': data.head(),
        'tail': data.tail()
    }
    print(summary)
    return summary

def find_differences(datalist):
    # Find differences between two DataFrames
    if len(datalist) < 2:
        raise ValueError("At least two DataFrames are required to find differences.")
    diff = pd.DataFrame()
    common_index = datalist[0].index.intersection(datalist[1].index)
    df1 =datalist[0].loc[common_index]
    df2 =datalist[1].loc[common_index]

    differences = df1.ne(df2)  # Element-wise comparison (not equal)
    rows_with_differences = differences.any(axis=1)  # Identify rows with any differences
    different_row_indices = df1.index[rows_with_differences]
    weekdays = [dt.day_name() for dt in different_row_indices]
    print(different_row_indices)
    print(len)
    print(weekdays)
    combined_df = pd.concat([df1.loc[different_row_indices],df2.loc[different_row_indices]], axis=1, keys=['new', 'old'])
    print(combined_df)


    return diff

[summarize_data(read_parquet_file(filename, bar_setting_folder, path)) for path in paths]
find_differences([read_parquet_file(filename, bar_setting_folder, path) for path in paths])