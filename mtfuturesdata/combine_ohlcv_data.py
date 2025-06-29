#These are a collection of functions to conbine two OHLCV dataframes of the same contract at the same frequency.
#I updated the code to retrieve IB data from the IB API. Specifically, I check to ensure that data is downloaded after the specified time frame has passed, thus avoiding the issue of 
# ddwnloading data that reflects partial trading activity for the specified time frame. 
#As one can only downloads data from IB for the last 2 years or so, this code is useful to combine IB data downloaded after the code update with data downloaded before the code update.
#The code considers that there is a primary dataframe (df1) and a secondary dataframe (df2), and it combines them such that the primary dataframe takes precedence over the secondary one.
#In this case the primary dataframe is the IB data downloaded after the code update, and the secondary dataframe is the IB data downloaded before the code update.
#It can potentially be used to combine data from different sources, such as IB and BarChart data 


import pandas as pd
import os

primary_ib_parquet_store = '/mnt/sda1/data/parquet/ib'
secondary_barchart_parquet_store = '/mnt/sda1/data/parquet/ib_historical'
sub_dirs = ['CTH_5_mins', 'CTH_15_mins', 'CTH_1_hour', 'RTH_1_day']    


def combine_two_ohlcv_dataframes(df1, df2, timezone='UTC'):
    result = df1.combine_first(df2)
    return result

def find_all_basename_of_parquet_files(directory):
    import os
    parquet_files = [f for f in os.listdir(directory) if f.endswith('.parquet')]
    return [os.path.splitext(f)[0] for f in parquet_files]

def combine_ohlcv_dataframes_for_one_frequency(primary_ib_parquet_store, secondary_barchart_parquet_store, frequency):
    primary_path = f"{primary_ib_parquet_store}/{frequency}"
    secondary_path = f"{secondary_barchart_parquet_store}/{frequency}"
    
    primary_filenames = find_all_basename_of_parquet_files(primary_path)
    secondary_filenames = find_all_basename_of_parquet_files(secondary_path)

    mutual_names = set(primary_filenames) & set(secondary_filenames)
    secondary_only_names = set(secondary_filenames) - set(primary_filenames)
    
    #print(f"Mutual files: {mutual_names}")
    #print("----------------------------")
    #print(f"Secondary only files: {secondary_only_names}")
    print("Number of mutual files:", len(mutual_names))
    print("Number of secondary only files:", len(secondary_only_names))

    i = 0
    for filename in mutual_names:
        print(f"Combining {filename}...")
        primary_file = f"{primary_path}/{filename}.parquet"
        secondary_file = f"{secondary_path}/{filename}.parquet"
        
        if os.path.exists(primary_file) and os.path.exists(secondary_file):
            df1 = pd.read_parquet(primary_file)
            df2 = pd.read_parquet(secondary_file)
            combined_df = combine_two_ohlcv_dataframes(df1, df2)
            if not df1.equals(combined_df):
                print(f"Dataframes for {filename} are not equal, saving combined dataframe.")
                i = i + 1
                combined_df.to_parquet(primary_file, index=True)
            #combined_df.to_parquet(primary_file, index=True)
    print(f"Total files combined: {i}")
    for filename in secondary_only_names:  
        secondary_file = f"{secondary_path}/{filename}.parquet"
        if os.path.exists(secondary_file):
            df2 = pd.read_parquet(secondary_file)
            #copy the secondary file to the primary path
            df2.to_parquet(f"{primary_path}/{filename}.parquet", index=True)

    return

def sort_combined_files(primary_ib_parquet_store, frequency):
    primary_path = f"{primary_ib_parquet_store}/{frequency}"
    filenames = find_all_basename_of_parquet_files(primary_path)
    
    for filename in filenames:
        file_path = f"{primary_path}/{filename}.parquet"
        df = pd.read_parquet(file_path)
        sorted_df = df.sort_index()
        if not df.equals(sorted_df):
            print(f"Sorting dataframe for {filename}...")
            sorted_df.to_parquet(file_path, index=True)
        else:
            print(f"Dataframe for {filename} is already sorted.")

#filenames = find_all_basename_of_parquet_files(f"{primary_ib_parquet_store}/{sub_dirs[0]}")
#print(filenames)
for sub_dir in sub_dirs:
    print(f"Combining data for frequency: {sub_dir}")
    #combine_ohlcv_dataframes_for_one_frequency(primary_ib_parquet_store, secondary_barchart_parquet_store, sub_dir)
    sort_combined_files(primary_ib_parquet_store, sub_dir)

