from csv import reader
import datetime as dt
from typing import Union


# Returns if the provided year is leap (has a 29th of Februry)
def is_leap_year(year : int) -> bool:
    if (year % 4 == 0 and year % 100 != 0) or (year % 4 == 0 and year % 400 ==0):
        return True
    else:
        return False

# Return True is the provided date is valid
# Date must be provided as a datetime.date or datetime.
def date_is_valid(date : Union[dt.date, dt.datetime]) -> bool:

    # Possible values
    if date.year < 1950 or date.year >2050:
        return False
    elif date.month < 1 or date.month > 12:
        return False
    elif date.day < 1 or date.day > 31:
        return False

    # Number of days in mounth
    if date.month in [4,6,9,11] and date.day == 31:
        return False
    elif date.month == 2 and date.day > 29:
        return False
    elif not is_leap_year(date.year) and date.month == 2 and date.day == 29:
        return False

    # Checking if date is in the futur
    today = dt.date.today()
    if not ending_date_is_superior(date, today):
        return False

    return True


# Returns True if the ending date is strictly after the starting date, or False
def ending_date_is_superior(start : Union[dt.date, dt.datetime], end : Union[dt.date, dt.datetime]) -> bool:
    if end.year < start.year:
        return False
    elif end.year == start.year and end.month < start.month:
        return False
    elif end.year == start.year and end.month == start.month and start.day >= end.day:
        return False
    else:
        return True

# Load a ticker list from file
def load_ticker_list(path : str):
    tickers = []
    with open(path, 'r') as csv_file:
        csv_reader = reader(csv_file)
        for row in csv_reader:
            tickers.append(row[0])
    return tickers
