import pandas as pd
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysobjects.multiple_prices import futuresMultiplePrices

def list_csv_multiple_prices_instruments(csv_multiple_prices_path):
    #list the instruments in a csv multiple prices folder 
    #for various debugging purposes 
    #Note that the parquet multiple prices should not be used for debugging and in general should be only one "production" version of multiple prices data

    csv_multiple_prices_data = csvFuturesMultiplePricesData(csv_multiple_prices_path)
    instruments = csv_multiple_prices_data.get_list_of_instruments()

    return instruments

def instrument_lastdate(csv_multiple_prices_path):
    #return a list of tuples, each of the form (instrument, last date in data), sort by date ascending
    csv_multiple_prices = csvFuturesMultiplePricesData(csv_multiple_prices_path)
    instruments = csv_multiple_prices.get_list_of_instruments()
    instrument_lastdate_list = []
    for instrument in instruments:
        multiple_prices_obj = data = csv_multiple_prices._get_multiple_prices_without_checking(instrument)
        last_date = multiple_prices_obj.index[-1]
        instrument_lastdate_list.append((instrument, last_date))
    instrument_lastdate_list.sort(key=lambda x: x[1])
    return instrument_lastdate_list

if __name__ == "__main__":

    spliced_multiple_prices_folder = '/mnt/sda1/pysystemtrade/data/futures/multiple_prices_csv_spliced/'
    list1 = list_csv_multiple_prices_instruments(spliced_multiple_prices_folder)

    repo_multiple_prices_folder = '/mnt/sda1/pysystemtrade/data/futures/multiple_prices_csv/'
    list2 = list_csv_multiple_prices_instruments(repo_multiple_prices_folder)

    instrument_lastdates = instrument_lastdate(spliced_multiple_prices_folder)
    print(instrument_lastdates)

    