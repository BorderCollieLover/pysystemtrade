#If a value is missing in the multiple prices file, but is present in the contract prices file, fill the missing value 

import pandas as pd
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysobjects.multiple_prices import futuresMultiplePrices
from sysdata.parquet.parquet_futures_per_contract_prices import parquetFuturesContractPriceData
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.config.production_config import get_production_config
from sysobjects.contracts import futuresContract


list_of_contract_names = [('CARRY', 'CARRY_CONTRACT'),
                       ('PRICE', 'PRICE_CONTRACT'),
                       ('FORWARD', 'FORWARD_CONTRACT')] 

futures_contract_parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
parquet_futures_contract_price_data = parquetFuturesContractPriceData(futures_contract_parquet_access)

def check_contract_price(instrument, date, contract_month):
    #check if the contract price data has a value for this date and contract month
    #if so return the value, else return None
    #This is a stub function, in real code it would access the contract prices data
    
    contract = futuresContract(instrument, contract_month)
    if parquet_futures_contract_price_data.has_merged_price_data_for_contract(contract):
        contract_price_data = parquet_futures_contract_price_data.get_merged_prices_for_contract_object(contract)
        if date in contract_price_data.index:
            price_value = contract_price_data.loc[date, 'FINAL']
            if not pd.isna(price_value):
                print(f"Checking contract price for {instrument} on {date} for contract month {contract_month}, found contract price: {price_value}")
                #print(f"Found contract price: {price_value}")
                return price_value
    
    return None

def fill_missing_multiple_prices_for_one_instrument(instrument, multiple_prices_obj):
    data_modified = False
    for price_header, contract_header in list_of_contract_names:
        for index in multiple_prices_obj.index:
            if pd.isna(multiple_prices_obj.loc[index, price_header]):
                contract_month = multiple_prices_obj.loc[index, contract_header]
                fill_value = check_contract_price(instrument, index, contract_month)
                if fill_value is not None:
                    multiple_prices_obj.at[index, price_header] = fill_value
                    data_modified = True
    return (data_modified, multiple_prices_obj)
        

def fill_missing_multiple_prices(csv_multiple_prices_path=None) :
    #fill missing multiple prices data from contract prices data
    if csv_multiple_prices_path:
        csv_multiple_prices = csvFuturesMultiplePricesData(csv_multiple_prices_path)
    else:
        csv_multiple_prices = csvFuturesMultiplePricesData()

    output_multiple_prices = csvFuturesMultiplePricesData('/mnt/sda1/tmp2')

    instruments = csv_multiple_prices.get_list_of_instruments()
    for instrument in instruments:
        print(f"Processing instrument {instrument}")
        multiple_prices_obj = csv_multiple_prices._get_multiple_prices_without_checking(instrument)
        data_modified, multiple_prices_obj = fill_missing_multiple_prices_for_one_instrument(instrument, multiple_prices_obj)
        if data_modified:
            output_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument, futuresMultiplePrices(multiple_prices_obj))

if __name__ == "__main__":
    fill_missing_multiple_prices()