import requests
import boto3
import sys
from awsglue.utils import getResolvedOptions
import datetime as dt

def is_leap_year(year):
    if (year % 4 == 0 and year % 100 != 0) or (year % 4 == 0 and year % 400 ==0):
        return True
    else:
        return False

#Return True is the provided date is valid
def date_is_valid(y, m, d):
    
    # Possible values
    if y < 1950 or y >2050:
        return False
    elif m < 1 or m > 12:
        return False
    elif d < 1 or d > 31:
        return False
        
    # Number of days in mounth
    if m in [4,6,9,11] and d == 31:
        return False
    elif m == 2 and d > 29:
        return False
    elif not is_leap_year(y) and m == 2 and d == 29:
        return False
        
    # Checking if date is in the futur
    today = dt.date.today()
    if not ending_date_is_superior(y, m, d, today.year, today.month, today.day):
        return False
        
    return True


# Returns True if the ending date is strictly after the starting date, or False
def ending_date_is_superior(sy, sm, sd, ey, em, ed):
    if ey < sy:
        return False
    elif ey == sy and em < sm:
        return False
    elif ey == sy and em == sm and sd >= ed:
        return False
    else:
        return True



if __name__ == '__main__':
    
    # Getting script args
    args = getResolvedOptions(sys.argv, ['ticker', 'starting_year', 'starting_month', 'starting_day', 'ending_year', 'ending_month', 'ending_day'])
    
    
    # Extracting args
    ticker = args['ticker']
    sy = int(args['starting_year'])
    sm = int(args['starting_month'])
    sd = int(args['starting_day'])
    ey = int(args['ending_year'])
    em = int(args['ending_month'])
    ed = int(args['ending_day'])
    
    # Checking if date are valids
    if not date_is_valid(sy, sm, sd):
        raise ValueError('Starting date is invalid')
    if not date_is_valid(ey, em, ed):
        raise ValueError('Ending date is invalid')
    
    # Checking correct date order
    if not ending_date_is_superior(sy, sm, sd, ey, em, ed):
        raise ValueError('Ending date must be bigger than starting date')
        
        
    URL = "https://cloud.iexapis.com/stable/stock/' + args['ticker'] + '/intraday-prices?token=pk_d4de3407f29a4f7897c3669c42f11a61&chartIEXOnly=true&exactDate=20191010"
    r = requests.get(url = URL)
    
    s3_client = boto3.client('s3',region_name='eu-west-2')
    s3_client.put_object(Body=r.text, Bucket='demo-rest-ingestion-bucket', Key= 'data.txt')
