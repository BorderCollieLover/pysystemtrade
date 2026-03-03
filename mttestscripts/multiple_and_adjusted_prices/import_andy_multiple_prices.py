#Compare the two multiple prices repositories: 
#One shipped by PST (latest ends around 2024.03)
#One shipped by bug-or-feature (plan to update regular)
import pandas as pd
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysobjects.multiple_prices import futuresMultiplePrices

MINIMUM_EXTRA_DATA_ROWS = 10 

def find_expired_price_contracts():
    #Fine out the times when price contract has already expired
    pst_multiple_csv_folder = '/mnt/sda1/pysystemtrade/data/futures/multiple_prices_csv/'
    #pst_multiple_csv_folder = '/mnt/sda1/pst-csv-data/data/multiple_prices_csv'
    csvMultiplePrices = csvFuturesMultiplePricesData(pst_multiple_csv_folder)
    list_of_codes = csvMultiplePrices.get_list_of_instruments()

    total_number_expiredness = 0
    total_number_contract_days = 0 
    for instrument in list_of_codes: 
        data = csvMultiplePrices._get_multiple_prices_without_checking(instrument)
        data['index_dt'] = [dt.strftime("%Y%m")+"00" for dt in data.index]
        #print(data)
        #print(sum(data['index_dt'] > data['PRICE_CONTRACT']))
        if sum(data['index_dt'] > data['PRICE_CONTRACT']) > 0: 
            comparison_result = data['index_dt'] > data['PRICE_CONTRACT']
            first_index = comparison_result.idxmin()
            total_number_expiredness += sum(data['index_dt'] > data['PRICE_CONTRACT'])
            total_number_contract_days += len(data)
            print(instrument, first_index) # instruments that hold expired price contracts and the first date. Multiple prices with expired price contracts should be discarded....... 
        #break
    print(total_number_expiredness, total_number_contract_days)


def compared_two_adjusted_prices_repos ():
    pst_adjusted_csv_folder = '/mnt/sda1/pysystemtrade/data/futures/adjusted_prices_csv/'
    csvAdjustedPrices = csvFuturesAdjustedPricesData(pst_adjusted_csv_folder)
    list_of_codes = csvAdjustedPrices.get_list_of_instruments()
    print(list_of_codes)

    andy_adjust_csv_folder = '/mnt/sda1/pst-csv-data/data/adjusted_prices_csv'
    csvAdjustedPrices2 = csvFuturesAdjustedPricesData(andy_adjust_csv_folder)
    list_of_codes = csvAdjustedPrices2.get_list_of_instruments()
    print(list_of_codes)

    output_data = pd.DataFrame()
    for instrument in list_of_codes: 
        rob_adjusted_prices = csvAdjustedPrices._get_adjusted_prices_without_checking(instrument)
        andy_adjusted_prices = csvAdjustedPrices2._get_adjusted_prices_without_checking(instrument)

        print(rob_adjusted_prices.index[0])
        print(andy_adjusted_prices.index[0])
        instrument_data = {'instrument': instrument, 'pst_start':rob_adjusted_prices.index[0] , 'andy_start': andy_adjusted_prices.index[0]}
        if output_data.empty: 
            output_data = pd.DataFrame(instrument_data, index=[0])
        else:
            output_data.loc[len(output_data)] = instrument_data

    return futuresMultiplePrices(output_data)

def patch_rob_multiple_prices_to_andy_multiple_prices(rob_multiple_prices, andy_multiple_prices):
    rob_start_dt = rob_multiple_prices.index[0]
    andy_start_dt = andy_multiple_prices.index[0]

    if (rob_start_dt < andy_start_dt):
        earlier_rob_dts = rob_multiple_prices.index[rob_multiple_prices.index<=andy_start_dt]

        last_dt = earlier_rob_dts[-1]
            
        if last_dt == andy_start_dt: 
        #ensures that the contracts are the same 
            if not ((rob_multiple_prices.loc[last_dt, 'PRICE_CONTRACT'] == andy_multiple_prices.loc[andy_start_dt, "PRICE_CONTRACT"]) and 
                    (rob_multiple_prices.loc[last_dt, 'CARRY_CONTRACT'] == andy_multiple_prices.loc[andy_start_dt, "CARRY_CONTRACT"]) and 
                    (rob_multiple_prices.loc[last_dt, 'FORWARD_CONTRACT'] == andy_multiple_prices.loc[andy_start_dt, "FORWARD_CONTRACT"])):
                print("Warning: different contracts for the same time")
        else: 
            if not ((rob_multiple_prices.loc[last_dt, 'PRICE_CONTRACT'] <= andy_multiple_prices.loc[andy_start_dt, "PRICE_CONTRACT"]) and
                    (rob_multiple_prices.loc[last_dt, 'CARRY_CONTRACT'] <= andy_multiple_prices.loc[andy_start_dt, "CARRY_CONTRACT"]) and
                    (rob_multiple_prices.loc[last_dt, 'FORWARD_CONTRACT'] <= andy_multiple_prices.loc[andy_start_dt, "FORWARD_CONTRACT"])): 
                print("Warning: later roll in Andy's contract")

        earlier_rob_data = rob_multiple_prices.loc[rob_multiple_prices.index<andy_start_dt]
        patched_data = pd.concat([earlier_rob_data, andy_multiple_prices])
        return patched_data
    else: 
        return andy_multiple_prices

def create_new_multiple_prices_from_rob_and_andy_repos(output_folder):
    #combine Rob and Andy's data to create an updated set of multiple prices 
    #For instruments that Andy has data:
    # 1. Use Andy's data if it starts earlier than Rob's, or if Rob has just a few (less than MINIMUM_EXTRA_DATA_ROWS) extra data points 
    # 2. Otherwise, patch earlier Rob data to Andy's data to create a patched up multiple prices, except for a few instances when we would still use Andy's data 
    # See Notion notes 

    pst_multiple_csv_folder = '/mnt/sda1/pysystemtrade/data/futures/multiple_prices_csv/'
    csvMultiplePrices = csvFuturesMultiplePricesData(pst_multiple_csv_folder)
    list_of_codes = csvMultiplePrices.get_list_of_instruments()
    print(list_of_codes)

    andy_multiple_csv_folder = '/mnt/sda1/pst-csv-data/data/multiple_prices_csv'
    csvMultiplePrices2 = csvFuturesMultiplePricesData(andy_multiple_csv_folder)
    andy_multiple_prices_codes = csvMultiplePrices2.get_list_of_instruments()
    print(list_of_codes)

    output_multiple_prices_csv_folder = output_folder
    combined_multiple_prices = csvFuturesMultiplePricesData(output_multiple_prices_csv_folder)
    special_instruments = ['NASDAQ_micro', 'LEANHOG', 'SOFR']

    for instrument in list_of_codes: 
        if instrument not in andy_multiple_prices_codes:
            rob_multiple_prices = csvMultiplePrices._get_multiple_prices_without_checking(instrument)
            combined_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument, rob_multiple_prices)
        else:
            rob_multiple_prices = csvMultiplePrices._get_multiple_prices_without_checking(instrument)
            andy_multiple_prices = csvMultiplePrices2._get_multiple_prices_without_checking(instrument)
            rob_start_dt = rob_multiple_prices.index[0]
            andy_start_dt = andy_multiple_prices.index[0]
            #print(rob_start_dt)
            #print(andy_start_dt)
            if (rob_start_dt > andy_start_dt):
                combined_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument, andy_multiple_prices)
            else:
                earlier_rob_dts = rob_multiple_prices.index[rob_multiple_prices.index<=andy_start_dt]
                if len(earlier_rob_dts) < MINIMUM_EXTRA_DATA_ROWS: 
                    combined_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument, andy_multiple_prices)
                else:
                    #patch earlier Rob data with Andy's data, except for a few instruments that should use Andy's data instead of patching
                    # as determined after manually checking the data 
                    if instrument in special_instruments: 
                        combined_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument, andy_multiple_prices)
                    else:
                        patched_multiple_prices = patch_rob_multiple_prices_to_andy_multiple_prices(rob_multiple_prices, andy_multiple_prices)
                        combined_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument, patched_multiple_prices)
    return


def interactive_check_rob_and_andy_multiple_prices():
    #This will go through the overlapping instruments as an initial check of whether earlier PST (Rob's) data can be patched to Andy's data
    pst_multiple_csv_folder = '/mnt/sda1/pysystemtrade/data/futures/multiple_prices_csv/'
    csvMultiplePrices = csvFuturesMultiplePricesData(pst_multiple_csv_folder)
    list_of_codes = csvMultiplePrices.get_list_of_instruments()
    print(list_of_codes)

    andy_multiple_csv_folder = '/mnt/sda1/pst-csv-data/data/multiple_prices_csv'
    csvMultiplePrices2 = csvFuturesMultiplePricesData(andy_multiple_csv_folder)
    list_of_codes = csvMultiplePrices2.get_list_of_instruments()
    print(list_of_codes)
    
    for instrument in list_of_codes: 
        rob_multiple_prices = csvMultiplePrices._get_multiple_prices_without_checking(instrument)
        andy_multiple_prices = csvMultiplePrices2._get_multiple_prices_without_checking(instrument)
        rob_start_dt = rob_multiple_prices.index[0]
        andy_start_dt = andy_multiple_prices.index[0]
        #print(rob_start_dt)
        #print(andy_start_dt)
        if (rob_start_dt < andy_start_dt):
            earlier_rob_dts = rob_multiple_prices.index[rob_multiple_prices.index<=andy_start_dt]
            if len(earlier_rob_dts) < MINIMUM_EXTRA_DATA_ROWS: 
                continue
            last_dt = earlier_rob_dts[-1]
            
            if last_dt == andy_start_dt: 
            #ensures that the contracts are the same 
                if not ((rob_multiple_prices.loc[last_dt, 'PRICE_CONTRACT'] == andy_multiple_prices.loc[andy_start_dt, "PRICE_CONTRACT"]) and 
                        (rob_multiple_prices.loc[last_dt, 'CARRY_CONTRACT'] == andy_multiple_prices.loc[andy_start_dt, "CARRY_CONTRACT"]) and 
                        (rob_multiple_prices.loc[last_dt, 'FORWARD_CONTRACT'] == andy_multiple_prices.loc[andy_start_dt, "FORWARD_CONTRACT"])):
                    print("Warning: different contracts for the same time")
            else: 
                if not ((rob_multiple_prices.loc[last_dt, 'PRICE_CONTRACT'] <= andy_multiple_prices.loc[andy_start_dt, "PRICE_CONTRACT"]) and
                        (rob_multiple_prices.loc[last_dt, 'CARRY_CONTRACT'] <= andy_multiple_prices.loc[andy_start_dt, "CARRY_CONTRACT"]) and
                        (rob_multiple_prices.loc[last_dt, 'FORWARD_CONTRACT'] <= andy_multiple_prices.loc[andy_start_dt, "FORWARD_CONTRACT"])): 
                    print("Warning: later roll in Andy's contract")
            print(instrument)
            print('Rob Repo Data: -----------------------------------------------')
            print(rob_multiple_prices.loc[last_dt])
            print('Andy Repo Data: ----------------------------------------------')
            print(andy_multiple_prices.iloc[0])
            input_str = ''
            while not ((input_str == 'Yes') or (input_str == 'No')):
                input_str = input("Patch Andy's data with PST repo? (Yes/No)")
            
             





if __name__ == "__main__": 
    #starting_dates = compared_two_adjusted_prices_repos()
    #starting_dates.to_csv('foo.csv')
    #patch_andy_multiple_prices()

    #find_expired_price_contracts()

    output_folder = '/mnt/sda1/pysystemtrade/mtdatastore/repodata/multiple_prices_csv_cleaned_and_combined_with_andy/'
    create_new_multiple_prices_from_rob_and_andy_repos(output_folder)

""


