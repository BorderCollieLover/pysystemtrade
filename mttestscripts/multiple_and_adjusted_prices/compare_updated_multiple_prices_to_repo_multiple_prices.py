from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
import os

if __name__ == "__main__":
    # Compare the last date of the multiple prices CSV files with the last date of the adjusted prices CSV files for each instrument. If they are not the same, print out which one is more recent and needs to be updated.
    repo_multiple_prices = csvFuturesMultiplePricesData()
    repo_instruments = repo_multiple_prices.get_list_of_instruments()
    print(repo_instruments)

    multiple_prices_from_db = os.path.join('data', 'futures', 'multiple_from_db')
    generated_multiple_prices = csvFuturesMultiplePricesData(multiple_prices_from_db)
    generated_instruments = generated_multiple_prices.get_list_of_instruments()
    print(generated_instruments)

    spliced_multiple_prices = os.path.join('data', 'futures', 'multiple_prices_csv_spliced')
    updated_multiple_prices = csvFuturesMultiplePricesData(spliced_multiple_prices)
    updated_instruments = updated_multiple_prices.get_list_of_instruments()
    print(updated_instruments)

    missing_in_generated = set(repo_instruments) - set(generated_instruments)
    missing_in_updated = set(repo_instruments) - set(updated_instruments)

    print(missing_in_generated)
    print(missing_in_updated)
