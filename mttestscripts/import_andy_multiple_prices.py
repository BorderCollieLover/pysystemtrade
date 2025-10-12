#Compare the two multiple prices repositories: 
#One shipped by PST (latest ends around 2024.03)
#One shipped by bug-or-feature (plan to update regular)
import pandas as pd
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData

MINIMUM_EXTRA_DATA_ROWS = 10 

def find_expired_price_contracts():
    #Fine out the times when price contract has already expired
    pst_multiple_csv_folder = '/mnt/sda1/pysystemtrade/data/futures/multiple_prices_csv/'
    pst_multiple_csv_folder = '/mnt/sda1/pst-csv-data/data/multiple_prices_csv'
    csvMultiplePrices = csvFuturesMultiplePricesData(pst_multiple_csv_folder)
    list_of_codes = csvMultiplePrices.get_list_of_instruments()

    total_number_expiredness = 0
    total_number_contract_days = 0 
    for instrument in list_of_codes: 
        data = csvMultiplePrices._get_multiple_prices_without_checking(instrument)
        data['index_dt'] = [dt.strftime("%Y%m")+"00" for dt in data.index]
        #print(data)
        #print(sum(data['index_dt'] > data['PRICE_CONTRACT']))
        total_number_expiredness += sum(data['index_dt'] > data['PRICE_CONTRACT'])
        total_number_contract_days += len(data)
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

    return output_data

def patch_andy_multiple_prices():
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
        andy_mutliple_prices = csvMultiplePrices2._get_multiple_prices_without_checking(instrument)
        rob_start_dt = rob_multiple_prices.index[0]
        andy_start_dt = andy_mutliple_prices.index[0]
        #print(rob_start_dt)
        #print(andy_start_dt)
        if (rob_start_dt < andy_start_dt):
            earlier_rob_dts = rob_multiple_prices.index[rob_multiple_prices.index<=andy_start_dt]
            if len(earlier_rob_dts) < MINIMUM_EXTRA_DATA_ROWS: 
                continue
            last_dt = earlier_rob_dts[-1]
            
            if last_dt == andy_start_dt: 
            #ensures that the contracts are the same 
                if not ((rob_multiple_prices.loc[last_dt, 'PRICE_CONTRACT'] == andy_mutliple_prices.loc[andy_start_dt, "PRICE_CONTRACT"]) and 
                        (rob_multiple_prices.loc[last_dt, 'CARRY_CONTRACT'] == andy_mutliple_prices.loc[andy_start_dt, "CARRY_CONTRACT"]) and 
                        (rob_multiple_prices.loc[last_dt, 'FORWARD_CONTRACT'] == andy_mutliple_prices.loc[andy_start_dt, "FORWARD_CONTRACT"])):
                    print("Warning: different contracts for the same time")
            else: 
                if not ((rob_multiple_prices.loc[last_dt, 'PRICE_CONTRACT'] <= andy_mutliple_prices.loc[andy_start_dt, "PRICE_CONTRACT"]) and
                        (rob_multiple_prices.loc[last_dt, 'CARRY_CONTRACT'] <= andy_mutliple_prices.loc[andy_start_dt, "CARRY_CONTRACT"]) and
                        (rob_multiple_prices.loc[last_dt, 'FORWARD_CONTRACT'] <= andy_mutliple_prices.loc[andy_start_dt, "FORWARD_CONTRACT"])): 
                    print("Warning: later roll in Andy's contract")
            print(instrument)
            print('Rob Repo Data: -----------------------------------------------')
            print(rob_multiple_prices.loc[last_dt])
            print('Andy Repo Data: ----------------------------------------------')
            print(andy_mutliple_prices.iloc[0])
            input_str = ''
            while not ((input_str == 'Yes') or (input_str == 'No')):
                input_str = input("Patch Andy's data with PST repo? (Yes/No)")
            
             





if __name__ == "__main__": 
    #starting_dates = compared_two_adjusted_prices_repos()
    #starting_dates.to_csv('foo.csv')
    patch_andy_multiple_prices()

    #find_expired_price_contracts()
""


