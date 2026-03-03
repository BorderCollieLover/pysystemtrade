from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysobjects.roll_calendars import rollCalendar


def remove_spurious_roll_from_roll_calendar_data(roll_calendar: rollCalendar) -> rollCalendar:
    """
    Remove spurious rolls from the given roll calendar for the specified instrument.
    The build_and_write_roll_calendar script considers the 
    Parameters:
    rollCalendar (csvRollCalendarData): The roll calendar data object.
    instrument (str): The instrument identifier to process.
    """

    if roll_calendar.empty:
        print(f"The roll calendar is empty. Skipping.")
        return
    
    spurious_roll = roll_calendar.iloc[-1]
    roll_calendar.drop(roll_calendar.index[-1], inplace=True)
    print(f"Removed spurious roll from calendar: {spurious_roll}")
    
    return (roll_calendar)




if __name__ == "__main__":
    tmp_roll_calendars_path = "data.futures.roll_calendars_from_db"
    instrument_code = "SP500"

    roll_calendars = csvRollCalendarData(tmp_roll_calendars_path)
    roll_calendar = roll_calendars._get_roll_calendar_without_checking(instrument_code)
    updated_roll_calendar = remove_spurious_roll_from_roll_calendar_data(roll_calendar)
    roll_calendars._add_roll_calendar_without_checking_for_existing_entry(instrument_code, updated_roll_calendar)

    
