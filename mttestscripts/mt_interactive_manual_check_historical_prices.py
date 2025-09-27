#This is a modified copy of the interactive_manual_check_historical_prices.py script
#The main change is to check all contracts for an instrument, not just the contracts that are marked as "sampling"

"""
Update historical data per contract from interactive brokers data, dump into mongodb

Apply a check to each price series
"""

from syscore.constants import success

from sysdata.data_blob import dataBlob
from sysproduction.data.prices import (
    diagPrices,
    get_valid_instrument_code_from_user,
)
from sysdata.tools.cleaner import interactively_get_config_overrides_for_cleaning
from sysproduction.update_historical_prices import (
    update_historical_prices_for_instrument,
)
from mttestscripts.test_seed_prices_from_ib import seed_price_data_from_IB


def interactive_manual_check_historical_prices():
    """
    Do a daily update for futures contract prices, using IB historical data

    If any 'spikes' are found, run manual checks

    :return: Nothing
    """
    with dataBlob(log_name="Update-Historical-prices-manually") as data:
        cleaning_config = interactively_get_config_overrides_for_cleaning(data=data)

        do_another = True
        while do_another:
            EXIT_STR = "Finished: Exit"
            instrument_code = get_valid_instrument_code_from_user(
                data, source="single", allow_exit=True, exit_code=EXIT_STR
            )
            if instrument_code is EXIT_STR:
                do_another = False
            else:
                check_instrument_ok_for_broker(data, instrument_code)
                data.log.debug(
                    "Updating log attributes",
                    method="clear",
                    instrument_code=instrument_code,
                )
                seed_price_data_from_IB(
                    instrument_code=instrument_code,
                    #cleaning_config=cleaning_config,
                    #data=data,
                    interactive_mode=True,
                )

    return success


def check_instrument_ok_for_broker(data: dataBlob, instrument_code: str):
    diag_prices = diagPrices(data)
    list_of_codes_all = diag_prices.get_list_of_instruments_with_contract_prices()
    if instrument_code not in list_of_codes_all:
        print("\n\n\ %s is not an instrument with price data \n\n" % instrument_code)
        raise Exception()


if __name__ == "__main__":
    interactive_manual_check_historical_prices()
