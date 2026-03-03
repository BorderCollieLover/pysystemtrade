from sysinit.futures.adjustedprices_from_db_multiple_to_db import process_adjusted_prices_single_instrument,process_adjusted_prices_all_instruments
from syscore.constants import arg_not_supplied
from syscore.constants import arg_not_supplied
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysobjects.adjusted_prices import futuresAdjustedPrices
from copy import copy
from sysproduction.data.prices import diagPrices
from sysobjects.multiple_prices import futuresMultiplePrices
import pandas as pd
import numpy as np

diag_prices = diagPrices()


def _get_data_inputs(csv_adj_data_path):
    db_multiple_prices = diag_prices.db_futures_multiple_prices_data
    db_adjusted_prices = diag_prices.db_futures_adjusted_prices_data
    csv_adjusted_prices = csvFuturesAdjustedPricesData(csv_adj_data_path)

    return db_multiple_prices, db_adjusted_prices, csv_adjusted_prices

def stitch_multiple_prices(
        multiple_prices: futuresMultiplePrices,
        forward_fill: bool = False,
    ):
        """
        Do backstitching of multiple prices using panama method

        If you want to change then override this method

        :param multiple_prices: multiple prices object
        :param forward_fill: forward fill prices and forwards before stitching

        :return: futuresAdjustedPrices

        """
        adjusted_prices = _panama_stitch(multiple_prices, forward_fill)
        return adjusted_prices


def _panama_stitch(
    multiple_prices_input: futuresMultiplePrices, forward_fill: bool = False
) -> pd.Series:
    """
    Do a panama stitch for adjusted prices

    :param multiple_prices:  futuresMultiplePrices
    :return: pd.Series of adjusted prices
    """
    multiple_prices = copy(multiple_prices_input)
    if forward_fill:
        multiple_prices.ffill(inplace=True)

    if multiple_prices.empty:
        raise Exception("Can't stitch an empty multiple prices object")

    previous_row = multiple_prices.iloc[0, :]
    adjusted_prices_values = [previous_row.PRICE]

    for dateindex in multiple_prices.index[1:]:
        current_row = multiple_prices.loc[dateindex, :]
        print(current_row)
        print(previous_row)

        if current_row.PRICE_CONTRACT == previous_row.PRICE_CONTRACT:
            # no roll has occured
            # we just append the price
            adjusted_prices_values.append(current_row.PRICE)
        else:
            # A roll has occured
            adjusted_prices_values = _roll_in_panama(
                adjusted_prices_values, previous_row, current_row
            )

        previous_row = current_row

    # it's ok to return a DataFrame since the calling object will change the
    # type
    adjusted_prices = pd.Series(adjusted_prices_values, index=multiple_prices.index)

    return adjusted_prices

def _roll_in_panama(adjusted_prices_values, previous_row, current_row):
    # This is the sort of code you will need to change to adjust the roll logic
    # The roll differential is from the previous_row
    roll_differential = previous_row.FORWARD - previous_row.PRICE
    if np.isnan(roll_differential):
        raise Exception(
            "On this day %s which should be a roll date we don't have prices for both %s and %s contracts"
            % (
                str(current_row.name),
                previous_row.PRICE_CONTRACT,
                previous_row.FORWARD_CONTRACT,
            )
        )

    # We add the roll differential to all previous prices
    adjusted_prices_values = [
        adj_price + roll_differential for adj_price in adjusted_prices_values
    ]

    # note this includes the price for the previous row, which will now be equal to the forward price
    # We now add todays price. This will be for the new contract

    adjusted_prices_values.append(current_row.PRICE)

    return adjusted_prices_values

def test_process_adjusted_prices_single_instrument():

    csv_adj_data_path = '/mnt/sda1/tmp'
    instrument_code = 'DOW'
    (
        db_multiple_prices,
        db_adjusted_prices,
        csv_adjusted_prices,
    ) = _get_data_inputs(csv_adj_data_path)
    multiple_prices = db_multiple_prices.get_multiple_prices(instrument_code)

    multiple_prices_copy = copy(multiple_prices)
    multiple_prices_copy.ffill(inplace=True)
    multiple_prices_copy.to_csv(f'{csv_adj_data_path}/multiple_prices.csv')

    adjusted_prices = stitch_multiple_prices(
        multiple_prices, forward_fill=True
    )
    adjusted_prices.to_csv(f'{csv_adj_data_path}/adjusted_prices.csv')


if __name__ == "__main__":
    #test_process_adjusted_prices_single_instrument()
    #process_adjusted_prices_single_instrument(instrument_code='DOW', csv_adj_data_path='/mnt/sda1/tmp', ADD_TO_CSV=True, ADD_TO_DB=False)
    process_adjusted_prices_all_instruments(
        csv_adj_data_path='/mnt/sda1/tmp2', ADD_TO_DB=True, ADD_TO_CSV=True
    )
