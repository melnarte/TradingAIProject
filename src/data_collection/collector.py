import datetime as dt
import requests
from utils import *

import os


class Collector():

    def __init__(self, data_path : str, api_token='pk_d4de3407f29a4f7897c3669c42f11a61'):
        self.data_path = data_path
        self.api_token = api_token

    # Retreive pricing data in 1 minute samples for a specific ticker and date from IEXCloud API
    def get_1d_minute_data(self,ticker : str, date : Union[dt.date, dt.datetime]):
        dateStr = date.strftime('%Y%m%d')
        url = 'https://cloud.iexapis.com/stable/stock/' + ticker + '/intraday-prices?token=' + self.api_token + "&chartIEXOnly=true&exactDate=" + dateStr
        r = requests.get(url)
        data = r.json()
        return data

    # Retreived data for a specific ticker over a time period from IEXCloud
    def get_ticker_data(self, ticker : str, start_date : Union[dt.date, dt.datetime], end_date : Union[dt.date, dt.datetime]):
        delta = dt.timedelta(days=1)
        date = start_date

        while(date != end_date):
            if(date.weekday() < 5):
                data = self.get_1d_minute_data(ticker, date)
                save_to_file(data, self.data_path, date, ticker)
            date += delta

if __name__ == '__main__':
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../data'))
    col = Collector(data_path)
    col.get_ticker_data('AAPL', dt.date(2020,10,1), dt.date(2020,10,6))
