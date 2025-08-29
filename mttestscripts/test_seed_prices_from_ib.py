from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysdata.config.production_config import get_production_config, Config
from sysdata.parquet.parquet_futures_per_contract_prices import CONTRACT_COLLECTION
from syscore.exceptions import missingData
from sysbrokers.IB.ib_futures_contract_price_data import (
    futuresContract,
)
from syscore.dateutils import DAILY_PRICE_FREQ, HOURLY_FREQ, Frequency
from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices
from sysproduction.data.broker import dataBroker
from sysproduction.data.prices import updatePrices
from sysproduction.update_historical_prices import write_merged_prices_for_contract
from random import shuffle
from sysdata.tools.cleaner import apply_price_cleaning


#MT: This is a fork of the seek_price_data_from_IB file from sysinit.futures. 
#Change the default action from overwriting to updating the price parquet objects 
#The only change is in 
#   seed_price_data_for_contract_at_frequency()
# to replace 
#   update_prices.overwrite_prices_at_frequency_for_contract()
# with 
#   update_prices.update_prices_at_frequency_for_contract()
#This script updates the historical data for all instruments and is expected to run weekly 
####################################################################################################################
####################################################################################################################
####################################################################################################################
##########USE THIS SCRIPT TO UPDATE ALL INSTRUMENTS HISTORICAL DATA#################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################

def seed_price_data_from_IB(instrument_code):
    data = dataBlob()
    data_broker = dataBroker(data)
    print(instrument_code)

    list_of_contracts = data_broker.get_list_of_contract_dates_for_instrument_code(
        instrument_code, allow_expired=True
    )
    print(list_of_contracts)

    ## This returns yyyymmdd strings, where we have the actual expiry date

    for contract_date in list_of_contracts:
        ## We do this slightly tortuous thing because there are energy contracts
        ## which don't expire in the month they are labelled with
        ## So for example, CRUDE_W 202106 actually expires on 20210528

        date_str = contract_date[:6]
        contract_object = futuresContract(instrument_code, date_str)

        seed_price_data_for_contract(data=data, contract_object=contract_object)


def seed_price_data_for_contract(data: dataBlob, contract_object: futuresContract):
    log_attrs = {**contract_object.log_attributes(), "method": "temp"}

    list_of_frequencies = [HOURLY_FREQ, DAILY_PRICE_FREQ]
    rows_added = 0 
    for frequency in list_of_frequencies:
        data.log.debug("Getting data at frequency %s" % str(frequency), **log_attrs)
        rows_added_for_frequency = seed_price_data_for_contract_at_frequency(
            data=data, contract_object=contract_object, frequency=frequency
        )
        rows_added += rows_added_for_frequency

    if rows_added > 0: 
        data.log.debug("Writing merged data for %s" % str(contract_object), **log_attrs)
        write_merged_prices_for_contract(
            data, contract_object=contract_object, list_of_frequencies=list_of_frequencies
        )


def seed_price_data_for_contract_at_frequency(
    data: dataBlob, contract_object: futuresContract, frequency: Frequency
):
    data_broker = dataBroker(data)
    update_prices = updatePrices(data)
    log_attrs = {**contract_object.log_attributes(), "method": "temp"}

    try:
        prices = (
            data_broker.get_prices_at_frequency_for_potentially_expired_contract_object(
                contract_object, frequency=frequency
            )
        )
    except missingData:
        data.log.warning(
            "Error getting data for %s" % str(contract_object),
            **log_attrs,
        )
        return 0
    
    daily_data = frequency is DAILY_PRICE_FREQ
    broker_prices = apply_price_cleaning(
            data=data,
            daily_data=daily_data,
            broker_prices_raw=prices,
        )
    prices = broker_prices

    data.log.debug(
        "Got %d lines of prices for %s" % (len(prices), str(contract_object)),
        **log_attrs,
    )

    if len(prices) == 0:
        data.log.warning(
            "No price data for %s" % str(contract_object),
            **log_attrs,
        )
        return 0 
    else:
        rows_added = update_prices.update_prices_at_frequency_for_contract(
            contract_object=contract_object, frequency=frequency, new_prices=prices, check_for_spike=False
        )
        if isinstance(rows_added, int):
            return rows_added
        else:
            return 0 




if __name__ == "__main__":
    FuturesInstrumentData = csvFuturesInstrumentData()
    config = Config()
    config = get_production_config()
    path = config.get_element("parquet_store")+'/'+CONTRACT_COLLECTION
    instruments = FuturesInstrumentData.get_list_of_instruments() # all instruments in PST
    #shuffle(instruments)
    #print(instruments)
    #instrument = 'GASOIL'
    #seed_price_data_from_IB(instrument)
    #exit()

    data = dataBlob(log_name="Update-Sampled_Contracts")
    diag_prices = diagPrices(data)
    instruments2 = diag_prices.get_list_of_instruments_in_multiple_prices() #instruments with multiple prices 
    #print(instruments2)
    less_important_instruments = list(set(instruments) - set(instruments2 ))
    #intruments = ['LEAD_LME']
    shuffle(instruments2)
    shuffle(less_important_instruments)
    for instrument in instruments2:
        try:
            seed_price_data_from_IB(instrument)
        except Exception as e:
            print(e)

    for instrument in less_important_instruments:
        try:
            seed_price_data_from_IB(instrument)
        except Exception as e:
            print(e)

