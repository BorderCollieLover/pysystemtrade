#This script will examine an OHLCV parquet file downloaded from IB, and add make it timezone aware if necessary. 
# The IB data downloading process has as the index datetime objects. For daily data, the hour and minute fields are set to 23 and 0, respectively. 
# The script will check the index of each OHLCV parquet file, if the index is not timezone aware, it will convert it to the UTC timezone

import pandas as pd
from pytz import timezone, utc
import datetime
import os


def read_ohlcv_parquet_file(filename):
    try:
        data = pd.read_parquet(filename)
    except Exception as e:
        data = None
        raise ValueError(f"Error reading parquet file {filename}: {e}")
    return data

def check_hour_minute_for_daily_data(data, hour_default = 23, minute_default = 0):
    if isinstance(data.index, pd.DatetimeIndex):
        if not ((data.index.hour == hour_default).all() and (data.index.minute == minute_default).all()):
            raise ValueError(f"Daily data should have an index with hour set to {hour_default} and minute set to {minute_default}.")
    else:
        raise ValueError("Daily data should have a DatetimeIndex.")

def ensure_timezone_awareness(filename, timezone='UTC'):
    #print(filename)
    data = read_ohlcv_parquet_file(filename)
    
    if data is None:
        return None
    
    if data.index.tz is not None:
        #print(f"Index is already timezone aware: {data.index.tz}")
        return None
    else:
        #print("Index is not timezone aware. Converting to UTC.")
        """ if isinstance(data.index, pd.Index) and any(isinstance(x, datetime.date) and not isinstance(x, datetime.datetime) for x in data.index):
            print("Index is of date type (not datetime)")
            return data 
        check_hour_minute_for_daily_data(data) """
        if isinstance(data.index, pd.DatetimeIndex):
            data.index = data.index.tz_localize('UTC')
        elif isinstance(data.index, pd.Index) and pd.api.types.is_datetime64_any_dtype(data.index):
            data.index = pd.to_datetime(data.index).tz_localize('UTC')
        else:
            raise ValueError("Index must be a datetime index or a datetime-like index.")
            return None
        data.to_parquet(filename, index=True)
        return None
    
def ensure_utc_for_all_ib_ohlcv_files(ibdatapath, timezone='UTC'):
    frequency_dirs = ['CTH_5_mins', 'CTH_15_mins', 'CTH_1_hour', 'RTH_1_day']

    for frequency_dir in frequency_dirs:
        full_path = f"{ibdatapath}/{frequency_dir}"
        try:
            filenames = [f"{full_path}/{filename}" for filename in os.listdir(full_path) if filename.endswith('.parquet')]
        except FileNotFoundError as e:
            print(f"Directory {full_path} not found: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error accessing directory {full_path}: {e}")
            continue
    
        for filename in filenames:
            try:
                ensure_timezone_awareness(filename, timezone)
            except ValueError as e:
                print(f"Error processing file {filename}: {e}")
            except Exception as e:
                print(f"Unexpected error processing file {filename}: {e}")

ib_data_path = '/mnt/sda1/data/parquet/ib'
ensure_utc_for_all_ib_ohlcv_files(ib_data_path)  