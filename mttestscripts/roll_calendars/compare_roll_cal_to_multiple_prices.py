# check roll calendar against the multiple prices data
# If the last roll entry has the same date as the last multiple price entry, 
# Then it is likely that the roll entry is spurious, an artifact from what the last data point is when running the script

from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData


def check_for_spurious_roll():
    roll_calendars = csvRollCalendarData()
    multiple_prices = csvFuturesMultiplePricesData()

    #print(roll_calendars.keys())
    #print(multiple_prices.keys())

    instruments = sorted(list(set(roll_calendars.keys()).intersection(set(multiple_prices.keys()))))

    spurious_counter = 0 
    for instrument in instruments:
        roll_calendar = roll_calendars.get_roll_calendar(instrument)
        multiple_price = multiple_prices.get_multiple_prices(instrument)

        last_roll_date = roll_calendar.index[-1]
        last_multi_date = multiple_price.index[-1]

        if last_roll_date == last_multi_date:
            print(f"Spurious roll entry found for {instrument} on {last_roll_date}")
            spurious_counter += 1
            roll_calendar.drop(roll_calendar.index[-1], inplace=True)
            roll_calendar.to_csv(f"/mnt/sda1/pysystemtrade/data/futures/roll_calendar_clean_spurious/{instrument}.csv")

    print(f"Total spurious roll entries found: {spurious_counter} for {len(instruments)} instruments")

    return

if __name__ == "__main__":
    check_for_spurious_roll()
    # This will print the keys of both roll calendars and multiple prices data
    # You can further implement logic to compare the last entries of both datasets  

