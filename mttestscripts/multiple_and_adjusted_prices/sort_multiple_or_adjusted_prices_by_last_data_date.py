import os
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData

csv_multiple_prices_path = os.path.join('data', 'futures', 'multiple_prices_csv_spliced')
csv_multiple_prices_data = csvFuturesMultiplePricesData(csv_multiple_prices_path)
multiple_price_instruments = csv_multiple_prices_data.get_list_of_instruments()
print(multiple_price_instruments)

dates_and_instruments = []
for instrument_code in multiple_price_instruments:
    multiple_prices_for_instrument = csv_multiple_prices_data._get_multiple_prices_without_checking(instrument_code)
    #print(instrument_code, multiple_prices_for_instrument.index[-1])
    dates_and_instruments.append((multiple_prices_for_instrument.index[-1], instrument_code))
sorted_dates_and_instruments = sorted(dates_and_instruments, key=lambda x: x[0])
print(sorted_dates_and_instruments)


    


