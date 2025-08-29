from sysquant.estimators.vol import robust_vol_calc
from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
from matplotlib.pyplot import show
from systems.accounts.account_forecast import pandl_for_instrument_forecast


def calc_ewmac_forecast(price, Lfast, Lslow=None): 
    """
    Calculate the ewmac trading rule forecast,
    Given a price and EWMA speeds Lfast, Lslow
    Adjusted by the vol_lookback
     
    Args:
        prices : This is the stitched price series 
        Lfast (int): The fast EWMA window length.
        Lslow (int, optional): The slow EWMA window length. If not provided, defaults to 4 * Lfast.
    """
    price = price.resample('1B').last()
    if Lslow is None: 
        Lslow = 4 * Lfast

    fast_ewma = price.ewm(span=Lfast).mean()
    slow_ewma = price.ewm(span=Lslow).mean()
    raw_ewmac = fast_ewma - slow_ewma

    vol = robust_vol_calc(price.diff())

    return raw_ewmac / vol

if __name__ == "__main__":
    
    data = csvFuturesSimData()

    instrument_code = 'SOFR'
    price = data.daily_prices(instrument_code)
    ewmac = calc_ewmac_forecast(price, 32)
    ewmac.tail(5)

    account = pandl_for_instrument_forecast(forecast = ewmac, price = price)
    print(account.stats())
