# 2025.09.04
# As noted https://www.notion.so/Data-Clean-Up-25e39604e82e805c8efdc33b7a2f8c2f?source=copy_link, my earlier data updating processes, in particular the one seeding data from IB, might need to be revisited
# As of 2025.09.04, I added the seeding data from IB code to ensure that the data is correctly populated from now on. 
# But earlier data might have the following issues: 1) "earlier" or partial bar data (which will affect H, L, C and Volume); 2) 0 volume bars in HOURLY data; 3) missing earlier data 
# The clean up procedures: 
# 1. remove all 0-volume bars from Hourly data; (and recompile the mixed frequency data if Hourly is changed)
# 2. For both hourly and daily data, update from the IB data if the PST contract key can be mapped to a unique IB conId. Still, will check for data consistency btwn two parquets before merging
# 2.a If there are more data in the merged dataset, then update the IB data parquet as well 
# 2.b If either hourly or daily data is changed, then recompile the mixed frequency data
# 3. Update FX data -- might as well just get a clean time series before backtest

