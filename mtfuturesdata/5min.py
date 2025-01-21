from mtfuturesdata.multiprocessingtest import get_effective_contracts_for_frequency, update_all_ohlcv_contracts_serial
freq = {'useRTH': False, 'barSizeSetting': '5 mins'}
all_effective_contracts = get_effective_contracts_for_frequency(**freq)
update_all_ohlcv_contracts_serial(all_effective_contracts)