import os
from sysinit.futures.multipleprices_from_db_prices_and_csv_calendars_to_db import process_multiple_prices_single_instrument
from mttestscripts.generate_rough_roll_calendar_updates_from_parquet_contract_prices import create_tmp_directories_for_update_roll_calendars, generate_spliced_multiple_prices



if __name__ == "__main__":
    ...
    roll_calendars_from_db, multiple_prices_from_db, spliced_multiple_prices, patched_roll_calendars = create_tmp_directories_for_update_roll_calendars()
    instrument_code = 'BB3M'
    process_multiple_prices_single_instrument(
        instrument_code,
        csv_multiple_data_path=multiple_prices_from_db, 
        ADD_TO_DB=False,
        csv_roll_data_path=roll_calendars_from_db,
        ADD_TO_CSV=True
    )
    generate_spliced_multiple_prices(instrument_code, multiple_prices_from_db, spliced_multiple_prices)