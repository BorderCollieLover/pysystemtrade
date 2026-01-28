from systems.provided.futures_chapter15.basesystem import futures_system
from sysdata.config.configdata import Config
from systems.trading_rules import TradingRule
from systems.provided.rules.ewmac import ewmac


if False:
    instrument="EUROSTX"
    system = futures_system()
    system.portfolio.get_notional_position(instrument)
    system.rules.get_raw_forecast(instrument, "ewmac64_256")
    system.accounts.portfolio().stats()

if False:
    my_config = Config("mtbacktest.backtest_system_1.futuresconfig.yaml")
    system = futures_system(config=my_config)

if False:
    system = futures_system()
    new_config = system.config
    print(new_config   )
    new_idm = 1.1
    new_config.instrument_div_multiplier = new_idm 
    system.config.volatility_calculation=dict(days=20)
    system = futures_system(config=new_config)

if False:
    system = futures_system()
    new_config = system.config
    new_weights = dict(SP500=0.5, KR10=0.5)
    new_idm = 1.1 
    new_config.instrument_weights = new_weights
    new_config.instrument_div_multiplier = new_idm
    system = futures_system(config=new_config)


if False:
    system = futures_system()
    config = system.config
    # method 1
    new_rule = TradingRule(
    dict(function="systems.provided.rules.ewmac", data=["rawdata.daily_prices", "rawdata.daily_returns_volatility"],
            other_args=dict(Lfast=10, Lslow=40)))

    # method 2 - good for functions created on the fly

    new_rule = TradingRule(dict(function=ewmac, data=["rawdata.daily_prices", "rawdata.daily_returns_volatility"],
                                other_args=dict(Lfast=10, Lslow=40)))

    ## both methods - modify the configuration
    config.trading_rules['new_rule'] = new_rule

    ## If you're using fixed weights and scalars

    #if forecast_scalars already defined
    #config.forecast_scalars['new_rule'] = 7.0
    #if forecast_scalars not defined yet
    config.forecast_scalars = dict(new_rule=7.0)
    config.forecast_weights = dict(ewmac16_64=0.16, ewmac32_128=0.08, ewmac64_256=0.16, carry=0.5, new_rule=0.10)  ## all existing forecast weights will need to be updated
    config.forecast_div_multiplier = 1.5

    ## If you're using estimates

    config.use_forecast_scale_estimates = True
    config.use_forecast_weight_estimates = True
    use_forecast_div_mult_estimates: True

    config.rule_variations = ['ewmac16_64', 'ewmac32_128', 'ewmac64_256', 'new_rule']
    # or to specify different variations for different instruments
    config.rule_variations = dict(SP500=['ewmac16_64', 'ewmac32_128', 'ewmac64_256', 'new_rule'], US10=['ewmac16_64', 'ewmac32_128', 'ewmac64_256','new_rule'])

if False:
    from sysdata.sim.csv_futures_sim_data import csvFuturesSimData

    data=csvFuturesSimData()

    ## getting data out
    data.methods() ## list of methods
    instrument_code="SP500"
    base_currency="USD"

    data.get_raw_price(instrument_code)
    data[instrument_code] ## does the same thing as get_raw_price

    data.get_instrument_list()
    data.keys() ## also gets the instrument list

    data.get_value_of_block_price_move(instrument_code)
    data.get_instrument_currency(instrument_code)
    data.get_fx_for_instrument(instrument_code, base_currency) # get FX rate between instrument currency and base currency

    ## using with a system
    from systems.provided.futures_chapter15.basesystem import futures_system
    system=futures_system(data=data)

    system.data.get_instrument_currency(instrument_code) # and so on


if True:
    from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
    from sysdata.sim.db_futures_sim_data import dbFuturesSimData
    instrument_code="SP500"

    ## with the default folders
    data=csvFuturesSimData()

    ## OR with different folders, by providing a dict containing the folder(s) to use
    data=csvFuturesSimData(csv_data_paths = dict(key_name = "pathtodata.with.dots"))

    # Permissible key names are 'csvFxPricesData' (FX prices), 'csvFuturesMultiplePricesData' 
    # (for carry and forward prices),
    # 'csvFuturesAdjustedPricesData' and 'csvFuturesInstrumentData' (configuration and costs).
    # If a keyname is not present then the system defaults will be used

    # An example to override with FX data stored in /psystemtrade/private/data/fxdata/:

    data=csvFuturesSimData(csv_data_paths = dict(csvFxPricesData="private.data.fxdata"))

    # WARNING: Do not store multiple_price_data and adjusted_price_data in the same directory
    #          They use the same file names!

    ## getting data out
    data.methods() ## will list any extra methods
    data.get_instrument_raw_carry_data(instrument_code) ## specific data for futures

    ## using with a system
    from systems.provided.futures_chapter15.basesystem import futures_system
    system=futures_system(data=data)
    system.data.get_instrument_raw_carry_data(instrument_code)

    data = dbFuturesSimData()
    system = futures_system(data=data)
    print(system.accounts.portfolio().stats())