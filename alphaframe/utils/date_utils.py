import datetime as dt
from dateutil.parser import parse


def to_date(string):
    if isinstance(string, str):
        return parse(string)
    else:
        return string

def opening_hour(date):
    return dt.datetime(date.year, date.month, date.day, hour=9, minute=30,second=0)

def closing_hour(date):
    return dt.datetime(date.year, date.month, date.day, hour=15, minute=59,second=0)

def get_clean_trading_dates(start, end):
    start = opening_hour(to_date(start))
    end = closing_hour(to_date(end))
    while start.weekday() > 4:
        start += dt.timedelta(days=1)
    while end.weekday() > 4:
        end -= dt.timedelta(days=1)

    if(end < start):
        print('ERROR : end date must be after start date !')

    return start,end
