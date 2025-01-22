#This script compares the repo roll calendar with the roll calendar generated from multiple_prices files
#, before using the latter to replace the former. 
#I want to make sure that the latter is indeed more updated than the former. 
#Once the roll calendars are set up, I can then update multiple_prices etc. 
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
import csv


def write_comparison_to_csv(comparison_list, csv_file_path):
    # Define the header for the CSV file
    header = ['instrument', 'repo_start_date', 'repo_end_date', 'mp_start_date', 'mp_end_date']
    
    # Open the CSV file for writing
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header
        writer.writerow(header)
        
        # Write the comparison data
        for instrument, comparison in comparison_list.items():
            row = [
                instrument,
                comparison['repo']['start_date'],
                comparison['repo']['end_date'],
                comparison['mp']['start_date'],
                comparison['mp']['end_date']
            ]
            writer.writerow(row)

def compare_roll_calendars():
    #First, get the roll calendars from the repo
    
    repo_roll_calendar_data = csvRollCalendarData()
    mp_roll_calendar_data = csvRollCalendarData(datapath='/mnt/sda1/pysystemtrade/mttestdata/')
    
    
    #Second, get the roll calendars from the multiple_prices files
    #multiple_prices_data = csvFuturesMultiplePricesData()
    #multiple_prices_roll_calendars = multiple_prices_data.get_roll_calendars()
    
    #Third, compare the two roll calendars
    summary_table = {}
    for instrument in repo_roll_calendar_data.keys():
        repo_roll_calendar = repo_roll_calendar_data.get_roll_calendar(instrument)
        if instrument in mp_roll_calendar_data.keys():
            mp_roll_calendar = mp_roll_calendar_data.get_roll_calendar(instrument)

            roll_calendar_comparison = {
                'repo': {
                    'start_date': repo_roll_calendar.index.min(),
                    'end_date': repo_roll_calendar.index.max()
                },
                'mp': {
                    'start_date': mp_roll_calendar.index.min(),
                    'end_date': mp_roll_calendar.index.max()
                }
            }
            summary_table[instrument] = roll_calendar_comparison
    return(summary_table)

if __name__ == "__main__":
    roll_calendar_comparison = compare_roll_calendars()
    print(roll_calendar_comparison)
    write_comparison_to_csv(roll_calendar_comparison, '/mnt/sda1/pysystemtrade/mttestdata/roll_calendar_comparison.csv')