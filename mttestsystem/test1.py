from systems.provided.futures_chapter15.basesystem import futures_system
system=futures_system()
system.rules.get_raw_forecast("DAX", "ewmac64_256")


from systems.provided.futures_chapter15.basesystem import futures_system
system=futures_system()
system.accounts.portfolio().stats() ## see some statistics
system.accounts.portfolio().curve().plot() ## plot an account curve
system.accounts.portfolio().percent.curve().plot() ## plot an account curve in percentage terms
system.accounts.pandl_for_instrument("US10").percent.stats() ## produce % statistics for a 10 year bond
system.accounts.pandl_for_instrument_forecast("SOFR", "carry").sharpe() ## Sharpe for a specific trading rule variation


from sysdata.config.configdata import Config
from systems.provided.futures_chapter15.basesystem import futures_system

#my_config=Config("private.this_system_name.config.yaml")
#system=futures_system(config=my_config)

from systems.provided.futures_chapter15.basesystem import futures_system
system=futures_system()
new_config=system.config

new_idm=1.1 ## new IDM

new_config.instrument_div_multiplier=new_idm

## Heres an example of how you'd change a nested parameter
## If the element doesn't yet exist in your config:

system.config.volatility_calculation=dict(days=20)

## If it does exist:
system.config.volatility_calculation['days']=20


system=futures_system(config=new_config)