import os
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysdata.parquet.parquet_multiple_prices import parquetFuturesMultiplePricesData
from sysdata.parquet.parquet_access import ParquetAccess
from sysdata.config.production_config import get_production_config, Config






csv_multiple_prices_path = os.path.join('data', 'futures', 'multiple_prices_csv')
csv_multiple_prices_data = csvFuturesMultiplePricesData(csv_multiple_prices_path)
print(csv_multiple_prices_data.get_list_of_instruments())

db_multiple_prices_parquet_access = ParquetAccess(get_production_config().get_element("parquet_store"))
db_multiple_prices = parquetFuturesMultiplePricesData(db_multiple_prices_parquet_access)

print(db_multiple_prices.get_list_of_instruments() )

instrument_code = 'BUTTER'

csv_multiple_prices_for_instrument = csv_multiple_prices_data._get_multiple_prices_without_checking(instrument_code)
print(f"Multiple prices for {instrument_code} from CSV:")
print(csv_multiple_prices_for_instrument)

db_multiple_prices_for_instrument = db_multiple_prices._get_multiple_prices_without_checking(instrument_code)
print(f"Multiple prices for {instrument_code} from DB:")
print(db_multiple_prices_for_instrument)    

db_multiple_prices._add_multiple_prices_without_checking_for_existing_entry(instrument_code, csv_multiple_prices_for_instrument)
print(f"Added multiple prices for {instrument_code} from CSV to DB.")