from mttestscripts.generate_rough_roll_calendar_updates_from_parquet_contract_prices import create_tmp_directories_for_update_roll_calendars
import pandas as pd 
from sysdata.config.production_config import get_production_config, Config
import os


#create a multipe price dataframe with a particular setting 
def create_multiple_prices_from_contracts(instrument, carry_contract, price_contract, forward_contract):
    carry_parquet = instrument+'#'+str(carry_contract)+'.parquet'
    price_parquet = instrument+'#'+str(price_contract)+'.parquet'
    forward_parquet = instrument+'#'+str(forward_contract)+'.parquet'

    config = Config()
    config = get_production_config()
    parquet_path = config.get_element("parquet_store")

    carry_parquet  = os.path.join(parquet_path,  'futures_contract_prices', carry_parquet)
    price_parquet  = os.path.join(parquet_path,  'futures_contract_prices', price_parquet)
    forward_parquet  = os.path.join(parquet_path,  'futures_contract_prices', forward_parquet)
    
    
    try:
        carry_ohlcv = pd.read_parquet(carry_parquet)
    except Exception as e: 
        print (e)

    
    price_ohlcv = pd.read_parquet(price_parquet)
    forward_ohlcv = pd.read_parquet(forward_parquet)

    #print(carry_ohlcv.head( ))      
    print(price_ohlcv.head( ))
    print(forward_ohlcv.head( ))
    print(price_ohlcv.tail()) 
    print(forward_ohlcv.tail())   
    
if __name__ == "__main__":
    create_multiple_prices_from_contracts('LEAD_LME', 20240500, 20250600, 20240700)

