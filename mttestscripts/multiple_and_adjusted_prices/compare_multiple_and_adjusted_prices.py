import pandas as pd
import os
from mttestscripts.files_tool import list_all_instruments_from_a_directory

def compare_adjusted_and_multiple_prices():
    #Check if the adjusted price and the multiple prices CSV files are in sync - if not, print out which one is more recent and needs to be updated
    multiple_prices_csv = os.path.join('data', 'futures', 'multiple_prices_csv')
    adjusted_prices_csv = os.path.join('data', 'futures', 'adjusted_prices_csv')

    assert os.path.exists(multiple_prices_csv), "The multiple prices CSV directory does not exist."
    assert os.path.exists(adjusted_prices_csv), "The adjusted prices CSV directory does not exist."

    instruments = list_all_instruments_from_a_directory(adjusted_prices_csv, extension='.csv')
    if not instruments: 
        print("No instruments found in the adjusted prices CSV directory.")
        return
    
    for instrument in instruments:
        multiple_prices_file = os.path.join(multiple_prices_csv, instrument)
        if not os.path.exists(multiple_prices_file):
            print(f"Multiple prices CSV for {instrument} does not exist. Skipping.")
            continue

        adjusted_prices_file = os.path.join(adjusted_prices_csv, instrument)

        multiple_prices = pd.read_csv(multiple_prices_file, index_col=0, parse_dates=True)
        adjusted_prices = pd.read_csv(adjusted_prices_file, index_col=0, parse_dates=True)

        last_adjusted_date = adjusted_prices.index[-1] if not adjusted_prices.empty else None
        last_multiple_date = multiple_prices.index[-1] if not multiple_prices.empty else None
        if last_adjusted_date is None or last_multiple_date is None:
            print(f"One of the files for {instrument} is empty. Skipping.")
            continue

        if last_adjusted_date < last_multiple_date:
            # If the last adjusted date is earlier than the last multiple prices date, we need to adjust the prices CSV
            print(f"Adjusting prices for {instrument}. Last adjusted date: {last_adjusted_date}, Last multiple prices date: {last_multiple_date}")
        else: 
            if last_adjusted_date > last_multiple_date:
            # If the last adjusted date is later than the last multiple prices date, we need to adjust the multiple prices CSV
                print(f"Adjusting multiple prices for {instrument}. Last adjusted date: {last_adjusted_date}, Last multiple prices date: {last_multiple_date}")
    return
