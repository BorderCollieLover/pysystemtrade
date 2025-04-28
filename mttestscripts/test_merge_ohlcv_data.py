#this script is used to test the full_merge_of_existing_data function and ascertain its behavior
#It didn't behave as I expected. 
#It seems that the functions focus on dealing with NAs in dataframes and series. 
#It doesn't seem to be a full merge of two dataframes when there are non-overlapping data. 
#Use combine_first to merge two dataframes and resolve conflicts.

from syscore.pandas.full_merge_with_replacement import full_merge_of_existing_data
import pandas as pd

def _merge_dataframes(df1, df2):
    # Merge two DataFrames on the 'timestamp' column
    merged_df = pd.merge(df1, df2, on='timestamp', how='outer', suffixes=('_df1', '_df2'))
    
    # Sort the merged DataFrame by 'timestamp'
    merged_df.sort_values(by='timestamp', inplace=True)
    
    return merged_df


filename = '/mnt/sda1/data/parquet/ib/CTH_5_mins/SR1N5_712984914.parquet'
data = pd.read_parquet(filename)
print(len(data))
data_10 = data.head(10)
data_5_15 = data.head(15).tail(10)
print(data_10)
print(data_5_15)
data_5_15_shift = data_5_15.shift(1)
print(data_5_15_shift)  


data_test_1 =  full_merge_of_existing_data(data_10, data_10)
print(data_test_1)
data_test_2 = full_merge_of_existing_data(data_10, data_5_15)
print(data_test_2)



# Example DataFrames
df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}, index=[0, 1, 2])
df2 = pd.DataFrame({'A': [None, 8, 9], 'B': [10, None, 12]}, index=[1, 2, 3])

# Concatenate and resolve conflicts
print(df2)
df2= df2.combine_first(df1)  
print(df2)
result = pd.concat([df1, df2]).sort_index()
result = result.loc[~result.index.duplicated(keep='last')]
print(df1)
print(df2)
print(result)

empty_1 = pd.DataFrame()
empty_2 = pd.DataFrame()
empty_2.combine_first(empty_1)
print(empty_1)
print(empty_2)
exit()

print(data_10.equals(data_test_1))
print(data_10.equals(data_test_2))

print(data_5_15)
print(data_5_15_shift)  
data_test_3 = full_merge_of_existing_data(data_5_15, data_5_15_shift)
print(data_test_3)

print(data_5_15)
print(data_5_15_shift)  
data_test_4 = full_merge_of_existing_data(data_5_15, data_5_15_shift, keep_older=False)
print(data_test_4)

test_empty_data = pd.DataFrame()
print(test_empty_data.equals(data_test_1))