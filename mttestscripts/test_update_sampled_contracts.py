#This is a test script to verify that the individual objects work as expected 

from syscore.exceptions import missingContract
from syscore.constants import success

from sysobjects.contract_dates_and_expiries import contractDate, expiryDate
from sysobjects.contracts import futuresContract, listOfFuturesContracts
from sysobjects.instruments import futuresInstrument
from sysobjects.rolls import contractDateWithRollParameters

from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices, get_valid_instrument_code_from_user
from sysproduction.data.contracts import dataContracts
from sysproduction.data.broker import dataBroker
from sysdata.config.production_config import get_production_config, Config
from sysdata.mongodb.mongo_futures_contracts import CONTRACT_COLLECTION 
import pymongo
from sysproduction.update_sampled_contracts import update_sampled_contracts,update_active_contracts_for_instrument,get_contract_chain,update_contract_database_with_contract_chain,update_expiries_and_sampling_status_for_contracts,check_key_contracts_have_not_expired,get_list_of_key_contract_ids
from sysproduction.update_sampled_contracts import has_contract_expired

def test1():
    data=dataBlob(log_name="Update-Sampled_Contracts")
    instrument_code = 'SP500'
    diag_prices = diagPrices(data)

    multiple_prices = diag_prices.get_multiple_prices(instrument_code)
    current_contract_dict = multiple_prices.current_contract_dict()
    furthest_out_contract_date = current_contract_dict.furthest_out_contract_date()

    diag_contract = dataContracts(data)
    roll_parameters = diag_contract.get_roll_parameters(instrument_code)
    furthest_out_contract = contractDateWithRollParameters(contractDate(furthest_out_contract_date), roll_parameters)
    print(furthest_out_contract)

    final_contract = furthest_out_contract.next_priced_contract()
    contract_date_chain = final_contract.get_contracts_from_recently_to_contract_date()

    instrument_object = futuresInstrument(instrument_code)
    contract_object_chain_as_list = [futuresContract(instrument_object, contract_date) for contract_date in contract_date_chain]

    data_contracts = dataContracts(data)
    labelled_dict_of_contracts = data_contracts.get_labelled_dict_of_current_contracts(instrument_code)

def test2():
    instrument_code='BB3M'
    data=dataBlob(log_name="Update-Sampled_Contracts")
    #update_active_contracts_for_instrument('BB3M', data)
    required_contract_chain = get_contract_chain(data, instrument_code)
    print(required_contract_chain)
    update_contract_database_with_contract_chain(
        instrument_code, required_contract_chain, data
    )
    update_expiries_and_sampling_status_for_contracts(
        instrument_code, data, contract_chain=required_contract_chain
    )
    #check_key_contracts_have_not_expired(instrument_code=instrument_code, data=data)
    key_contract_ids = get_list_of_key_contract_ids(
        instrument_code=instrument_code, data=data
    )
    print(key_contract_ids)

    

    for contract_id in key_contract_ids:
        data_contracts = dataContracts(data)
        contract = futuresContract(instrument_code, contract_id)
        actual_contract = data_contracts.get_contract_from_db(contract)

        

def test3():
    data=dataBlob(log_name="Update-Sampled_Contracts")
    diag_prices = diagPrices(data)
    list_of_codes = diag_prices.get_list_of_instruments_in_multiple_prices()
    for instrument_code in list_of_codes:
        key_contract_ids = get_list_of_key_contract_ids(
            instrument_code=instrument_code, data=data
        )
        for contract_id in key_contract_ids:
            data_contracts = dataContracts(data)
            contract = futuresContract(instrument_code, contract_id)
            try:
                actual_contract = data_contracts.get_contract_from_db(contract)
            except Exception as e:
                print(e)





if __name__ == "__main__":
    #test1()
    test3()  # Uncomment to run additional tests
















































































































































































































































































































































































































