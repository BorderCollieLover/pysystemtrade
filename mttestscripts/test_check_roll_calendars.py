from sysdata.csv.csv_roll_calendars import csvRollCalendarData

def check_all_roll_calendars():
    roll_calendar_data = csvRollCalendarData()
    for instrument in sorted(roll_calendar_data.keys()):
        print(f"Checking roll calendar for {instrument}")
        # Add your checks here
        roll_calendar = roll_calendar_data._get_roll_calendar_without_checking(instrument)
        check_result = roll_calendar.check_if_date_index_monotonic()
        if not check_result:
            print(f"Roll calendar for {instrument} has non-monotonic date index.")


if __name__ == "__main__":
    check_all_roll_calendars()