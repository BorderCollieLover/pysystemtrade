#Some tools used to clean up the contract price ohlc parquets
#When writing more tools, try to structure the code as a generic operation on an OHLC, and a dedicated routine specific for IB contract prices parquet
#The generic operation on an OHLC can thus be used to clean up PST contract prices data parquets if necessary 

#1. When there are blocks of data that are off by x10s, x100s, etc, 
#2. Duplicated rows 
#3. Inaccurate highs and lows (e.g. after fixing spikes)

import glob
import pandas as pd
import os
from mttestscripts.files_tool import list_all_instruments_from_a_directory
import pickle
import datetime
ib_parquet_folders = ['/mnt/sda1/data/parquet/ib/RTH_1_day/', '/mnt/sda1/data/parquet/ib/CTH_1_hour/', '/mnt/sda1/data/parquet/ib/CTH_15_mins/', '/mnt/sda1/data/parquet/ib/CTH_5_mins/']


##### IB OHLC Parquet Data Spike Cleaning Special Cases ############
#1. TU[FGHJKMNQUVXZ]2[0-9]*_*.parquet : value > 20000, scale by 10^-3 
#2. ZR[FGHJKMNQUVXZ][0-9]*_*.parquet : value <1 , scale by 10^2 
#3. UIN[FGHJKMNQUVXZ][0-9]*_*.parquet: value > 1000, scale by 10^-4
#4. TWR[FGHJKMNQUVXZ][0-9]*_*.parquet : value <2, scale by 10 
#5. [MWL|MPP|MMN|MLE|MFU]U2_*.parquet:  value < 100, scale by 100
#6. 30[CJ][FGHJKMNQUVXZ]2_*.parquet: value < 1.5, scale by 100 
#7. [EU9|JPP][U2|Z2]_*.parquet: value < 100, scale by 100  (similar to Rule #5 )
#8. ASN[FGHJKMNQUVXZ][0-9]_*.parquet: value < 10, scale by 100

# For each special case, find the relevant parquets, find the values that are above/below the threshold, and scale it with the scale_factor
# This method is useful when there are blocks of data that are simply off by 10s, 100s, 1000s etc 
# But these cases are identified after after examining the data manually, by finding and then trying to fix spikes first 
# The contract filters are defined using BASH command line-style wildcards 
IB_OHLC_Spike_Clean_Up_Cases = [
    {'contract_filter': 'TU[FGHJKMNQUVXZ]2[0-9]*_*.parquet', 'value_filter': 20000, 'greater_than': True, 'scale_factor': 10**-3},
    {'contract_filter': 'ZR[FGHJKMNQUVXZ][0-9]*_*.parquet', 'value_filter': 1, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'UIN[FGHJKMNQUVXZ][0-9]*_*.parquet', 'value_filter': 1000, 'greater_than': True, 'scale_factor': 10**-4},
    {'contract_filter': 'UIN[FGHJKMNQUVXZ][0-9]*_*.parquet', 'value_filter': 1, 'greater_than': False, 'scale_factor': 10**4},
    {'contract_filter': 'TWR[FGHJKMNQUVXZ][0-9]*_*.parquet', 'value_filter': 2, 'greater_than': False, 'scale_factor': 10},
    {'contract_filter': '30[CJ][FGHJKMNQUVXZ]2_*.parquet', 'value_filter': 1.5, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'ASN[FGHJKMNQUVXZ][0-9]_*.parquet', 'value_filter': 10, 'greater_than': False, 'scale_factor': 10**2},
    
    {'contract_filter': 'NGFK6_*.parquet', 'value_filter': 10, 'greater_than': True, 'scale_factor': 10**-2},
    {'contract_filter': 'MWLU2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'MPPU2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'MMNU2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'MLEU2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'MFUU2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'EU9U2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'EU9Z2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'JPPU2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
    {'contract_filter': 'JPPZ2_*.parquet', 'value_filter': 100, 'greater_than': False, 'scale_factor': 10**2},
]

#Use glob to generate a list of files using BASH command line-style wildcards 
def find_files_by_filter(folder, filter_str):
    all_files = glob.glob(os.path.join(folder, filter_str))
    return(all_files)

#Takes in an ohlc, and scale the values according to the rules
def clean_up_ohlc(ohlc, value_filter, greater_than, scale_factor, price_columns = ['open', 'high', 'low', 'close']):
    if ohlc is None: 
        return ohlc
    
    if ohlc.empty: 
        return ohlc
    
    result = ohlc.copy()
    #While it might seem cumbersome (and slow) to loop through the index and columns 
    #Other approaches triggers nasty SettingWithCopyWarnings  
    if greater_than:
        for index_value in result.index:
            for column in price_columns:
                if result.loc[index_value, column] > value_filter:
                    result.loc[index_value, column] = result.loc[index_value, column]*scale_factor
    else:
        for index_value in result.index:
            for column in price_columns:
                if result.loc[index_value, column] < value_filter:
                    result.loc[index_value, column] = result.loc[index_value, column]*scale_factor
  
    return result

#Clean up all IB contract price parquets where the above clean-up rules are applicable 
def ib_contract_prices_parquet_special_cases_clean_up():
    for folder in ib_parquet_folders:
        for rule in IB_OHLC_Spike_Clean_Up_Cases:
            all_files = find_files_by_filter(folder, rule['contract_filter'])
            for file in all_files: 
                ohlc = pd.read_parquet(file)
                ohlc_cleaned = clean_up_ohlc(ohlc, rule['value_filter'], rule['greater_than'], rule['scale_factor'])
                if ohlc.equals(ohlc_cleaned):
                    print("%s no data changed" % str(file))
                else: 
                    print("%s data changed" % str(file))
                    ohlc_cleaned.to_parquet(file)

#remove duplicated rows 
def remove_duplicated_rows_from_ohlc(ohlc_data):
    if ohlc_data is None:
        return ohlc_data
    
    if ohlc_data.empty: 
        return ohlc_data
    
    if len(ohlc_data)<2: 
        return ohlc_data 
    
    if not ohlc_data.index.is_monotonic_increasing:
        print('Data dates are not increasing!!!')

    if not ohlc_data.index.is_unique:
        ohlc_data_deduped = ohlc_data.loc[~ohlc_data.index.duplicated(keep='first')]
        return ohlc_data_deduped
    else:
        return ohlc_data
    
def remove_duplicated_rows_from_a_parquet(parquet_file, overwrite=False):

    if not os.path.isfile(parquet_file):
        print('%s is not a file. ' %(str(parquet_file)))
        return 
    
    try:
        ohlc_data = pd.read_parquet(parquet_file)
    except Exception as e: 
        print (e)
        return
    
    if not ohlc_data.index.is_unique:
        ohlc_deduped = remove_duplicated_rows_from_ohlc (ohlc_data)
        if overwrite: 
            ohlc_deduped.to_parquet(parquet_file)

    return

def dedup_all_ib_parquets():
    for folder in ib_parquet_folders: 
        all_parquets = list_all_instruments_from_a_directory(folder, extension='parquet')
        for parquet in all_parquets: 
            full_file_path = os.path.join(folder, parquet)
            remove_duplicated_rows_from_a_parquet(full_file_path, overwrite=True)

    return


def find_all_ib_futures_with_zero_volumes():
    parquet_folder = '/mnt/sda1/data/parquet/ib/RTH_1_day/'
    all_parquets = list_all_instruments_from_a_directory(parquet_folder, '.parquet')
    i = 0 
    for parquet in all_parquets:
        full_file_path = os.path.join (parquet_folder, parquet)
        data = pd.read_parquet(full_file_path)
        total_volume = sum(data['volume'])
        if total_volume == 0:
            print(full_file_path)
            i = i + 1 
    print(i)
    return


def generate_all_parquets_list_for_an_ib_parquet_filter(ib_parquet_filter):
    all_files = []
    for folder in ib_parquet_folders:
        files_in_folder = glob.glob(os.path.join(folder, ib_parquet_filter))
        all_files = all_files + files_in_folder
    return all_files

#return the total volume of an ohlc data. 
def return_total_volume(ohlc, volume_col='volume'):
    if ohlc is None:
        return True
    
    if ohlc.empty: 
        return True
    
    assert volume_col in ohlc.columns 
    return sum(ohlc[volume_col])


#check if the last datetime index value is at least n-days ago 
#strictly speaking not really check for expiry but close enough 
def test_for_expiry_past_n_days(ohlc, days_past=30):
    if ohlc is None:
        return True
    
    if ohlc.empty: 
        return True
    
    last_datetime = ohlc.index[-1]
    current_dt_aware = datetime.datetime.now(datetime.timezone.utc)

    # Calculate the n-day threshold
    n_days_ago = current_dt_aware - datetime.timedelta(days=days_past)
    return last_datetime <= n_days_ago

def test_index_for_all_ib_parquets():
    file_counter= 0 
    for folder in ib_parquet_folders: 
        all_parquets = list_all_instruments_from_a_directory(folder, extension='parquet')
        for parquet in all_parquets: 
            full_file_path = os.path.join(folder, parquet)
            data = pd.read_parquet(full_file_path)
            try:
                test_for_expiry_past_n_days(data)
            except Exception as e: 
                print(full_file_path)
                file_counter += 1 
    return file_counter

    
#re-calcuate the high/low value of an ohlc
#this is used to fix high/low values that may have been changed when fixing spikes 
def fix_high_low_for_ohlc(ohlc_data, price_columns = ['open', 'high', 'low', 'close']):
    if ohlc_data is None: 
        return False
    
    if ohlc_data.empty: 
        return False
      
    ohlc_price_data = ohlc_data[price_columns]
    result = ohlc_data.copy()
    high_column = price_columns[1]
    low_column = price_columns[2]
    result[high_column] = ohlc_price_data.max(axis=1, skipna=True)
    result[low_column] = ohlc_price_data.min(axis=1, skipna=True)
    return result

def fix_high_low_for_a_parquet(parquet_file, price_columns = ['open', 'high', 'low', 'close'], overwrite=False):
    if not os.path.isfile(parquet_file):
        print('%s is not a file. ' %(str(parquet_file)))
        return 
    
    try:
        ohlc_data = pd.read_parquet(parquet_file)
    except Exception as e: 
        print (e)
        return
    
    fixed_data  = fix_high_low_for_ohlc(ohlc_data, price_columns)
    if not ohlc_data.equals(fixed_data):
        if overwrite:
            fixed_data.to_parquet(parquet_file)
            print('Fixing high low for '+parquet_file)
    return

def fix_high_low_for_all_ib_parquets():
    for folder in ib_parquet_folders: 
        all_parquets = list_all_instruments_from_a_directory(folder, extension='parquet')
        for parquet in all_parquets: 
            full_file_path = os.path.join(folder, parquet)
            fix_high_low_for_a_parquet(full_file_path, overwrite=True)


if __name__ == "__main__":
    #ib_contract_prices_parquet_special_cases_clean_up()
    #dedup_all_ib_parquets()
    #find_all_ib_futures_with_zero_volumes()

    #problems = test_index_for_all_ib_parquets()
    #print(problems)

    fix_high_low_for_all_ib_parquets()
    exit()
    
    
    fsmx_filter = 'FSMX 202[34]*_*.parquet'
    fsmx_files = generate_all_parquets_list_for_an_ib_parquet_filter(fsmx_filter)
    pickle_name = '/mnt/sda1/foo.pkl'
    with open (pickle_name, 'wb') as file: 
        pickle.dump(fsmx_files, file)

    fsmx_filter = 'ECO[A-Z][0-9]_*.parquet'
    fsmx_files = generate_all_parquets_list_for_an_ib_parquet_filter(fsmx_filter)
    fsmx_files = ['/mnt/sda1/data/parquet/ib/RTH_1_day/ECOG8_803750278.parquet']
    for file in fsmx_files: 
        data = pd.read_parquet(file)
        print(file)
        print(data.tail(1))
        print(data.index[-1])
        
        try:
            print(data.index[-1]< datetime.datetime.now(datetime.timezone.utc))
            print(test_for_expiry_past_n_days(data))
            print(return_total_volume(data))
            data.index = data.index.tz_localize('UTC')
        except Exception as e: 
            print(str(e))