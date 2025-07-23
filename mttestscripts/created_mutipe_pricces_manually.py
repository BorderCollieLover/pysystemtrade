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

    carry_ohlcv['CARRY'] = carry_ohlcv['FINAL']
    price_ohlcv['PRICE'] = price_ohlcv['FINAL']
    forward_ohlcv['FORWARD']=forward_ohlcv['FINAL']
    multiple_prices = carry_ohlcv[['CARRY']].join(price_ohlcv[['PRICE']].join(forward_ohlcv[['FORWARD']], how='outer'), how='outer')
    multiple_prices['CARRY_CONTRACT'] = carry_contract
    multiple_prices['PRICE_CONTRACT'] = price_contract
    multiple_prices['FORWARD_CONTRACT'] = forward_contract
    multiple_prices = multiple_prices[['CARRY', 'CARRY_CONTRACT', 'PRICE','PRICE_CONTRACT','FORWARD', 'FORWARD_CONTRACT' ]]
    

    print(multiple_prices.head())
    print(multiple_prices.tail())
    return multiple_prices
    

    
if __name__ == "__main__":
    for instrument in ['LEAD_LME', 'TIN_LME', 'ZINC_LME']:
        generated_multiple_prices = create_multiple_prices_from_contracts(instrument, 20240800, 20240700, 20240800)
        generated_multiple_prices.to_csv(instrument+'.csv')
