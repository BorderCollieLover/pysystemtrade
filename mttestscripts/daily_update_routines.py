#This is to be run daily, and updates only "active" contract data with multiple_prices files
# by calling functions from update_sampled_contracts and update_historical_prices
#Maybe I should use this code to update all contracts ..... 

from asyncio import log
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from sysdata.data_blob import dataBlob
from sysproduction.update_sampled_contracts import  update_active_contracts_for_instrument
from sysdata.config.production_config import get_production_config, Config
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysproduction.update_historical_prices import update_historical_prices
from mttestscripts.add_all_contracts_to_db import update_expiries_and_sampling_status_for_multiple_prices_contracts
from datetime import datetime
from sysproduction.backup_db_to_csv import *

def update_contracts():
    FuturesInstrumentData = csvFuturesInstrumentData()
    config = Config()
    config = get_production_config()
    instruments = FuturesInstrumentData.get_list_of_instruments()
    #instruments = ['ALUMINUM']

    #add contracts to DB
    with dataBlob(log_name="Update-Sampled_Contracts") as data:
        for instrument in instruments:
            try:
                update_active_contracts_for_instrument(instrument,data)
            except Exception as e:
                print(e)

        
    #update expiries and sampling status
    with dataBlob(log_name="Update-Sampled_Contracts") as data:
        for instrument in instruments:
            try:
                update_expiries_and_sampling_status_for_multiple_prices_contracts(data, instrument)
            except Exception as e:
                print(e)

def mt_backup_db_to_csv():
    backup_data = dataBlob(log_name="backup_db_to_csv")

    try:
        backup_adj_to_csv(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_futures_contract_prices_to_csv(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_spreads_to_csv(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_fx_to_csv(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_multiple_to_csv(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_strategy_position_data(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_contract_position_data(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_historical_orders(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_capital(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_contract_data(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_spread_cost_data(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_optimal_positions(backup_data)
    except Exception as e:
        print(e)

    try:
        backup_roll_state_data(backup_data)
    except Exception as e:
        print(e)

    #log.debug("Copying to backup directory")
    try:
        backup_csv_dump(backup_data)
    except Exception as e:
        print(e)

def mt_run_backups():
    from sysproduction.backup_mongo_data_as_dump import backup_mongo_data_as_dump
    from sysproduction.backup_state_files import backup_state_files
    from sysproduction.backup_parquet_data_to_remote import backup_parquet_data_to_remote
    
    mt_backup_db_to_csv()
    backup_mongo_data_as_dump()
    backup_state_files()
    backup_parquet_data_to_remote()

def mt_update_repo_multi_adjusted_csvs():
    from mttestscripts.generate_rough_roll_calendar_updates_from_parquet_contract_prices import backup_repo_data
    backup_repo_data()

    from sysproduction.data.prices import diagPrices
    from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
    from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
    diag_prices = diagPrices()

    db_multiple_prices = diag_prices.db_futures_multiple_prices_data
    db_adjusted_prices = diag_prices.db_futures_adjusted_prices_data

    csv_adjusted_prices = csvFuturesAdjustedPricesData()
    csv_multiple_prices = csvFuturesMultiplePricesData()

    instrument_list = db_multiple_prices.get_list_of_instruments()
    for instrument_code in instrument_list:
        print(instrument_code)
        multiple_prices = db_multiple_prices.get_multiple_prices(instrument_code)
        csv_multiple_prices.add_multiple_prices(instrument_code, multiple_prices, ignore_duplication=True)

    instrument_list = db_adjusted_prices.get_list_of_instruments()
    for instrument_code in instrument_list:
        print(instrument_code)
        multiple_prices = db_adjusted_prices.get_adjusted_prices(instrument_code)
        csv_adjusted_prices.add_adjusted_prices(instrument_code, multiple_prices, ignore_duplication=True)

def mt_update_repo_fx_csvs():
    from sysproduction.data.currency_data import fxPricesData
    from sysdata.csv.csv_spot_fx import csvFxPricesData

    db_fx_prices_data = fxPricesData()
    csv_fx_prices = csvFxPricesData()

    list_of_ccy_codes = csv_fx_prices.get_list_of_fxcodes()

    for currency_code in list_of_ccy_codes:
        fx_prices = db_fx_prices_data.get_fx_prices(currency_code)
        csv_fx_prices.add_fx_prices(currency_code, fx_prices, ignore_duplication=True)


if __name__ == "__main__":

    #update contracts 
    update_contracts()

    #update repo csvs with latest db data
    mt_update_repo_multi_adjusted_csvs()
    mt_update_repo_fx_csvs()

    #backup database
    mt_run_backups()

    #update all sampled contracts
    update_historical_prices()

    


