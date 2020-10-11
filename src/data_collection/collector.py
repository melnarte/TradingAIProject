import datetime as dt
import requests
from tqdm import tqdm
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
        day_count = (end_date - start_date).days

        for d in (start_date + dt.timedelta(n) for n in tqdm(range(day_count))):
            if(d.weekday() < 5):
                data = self.get_1d_minute_data(ticker, d)
                save_to_file(data, self.data_path, d, ticker)

    # Retreive data for tickers list from IEXCloud
    def request_historical_data(self, start_date : Union[dt.date, dt.datetime], end_date : Union[dt.date, dt.datetime], fromIndex : int =0, nbTickers : int = 1):

        # Checking if date are valids
        if not date_is_valid(start_date):
            raise ValueError('Starting date is invalid')
        if not date_is_valid(end_date):
            raise ValueError('Ending date is invalid')

        # Checking correct date order
        if not ending_date_is_superior(start_date, end_date):
            raise ValueError('Ending date must be bigger than starting date')

        # Loading ticker list
        tickers = load_ticker_list(os.path.join(self.data_path, 'tickers/list.txt'))
        n = len(tickers)
        if nbTickers < 1:
            nbTickers = n

        for i in range(fromIndex, fromIndex+nbTickers):
            print("Getting " + tickers[i] + "(" + str(i+1) + "/" + str(n) + ")" "...")
            self.get_ticker_data(tickers[i], start_date, end_date)


if __name__ == '__main__':
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../data'))
    col = Collector(data_path)
    col.request_historical_data(dt.date(2020,10,1), dt.date(2020,10,6), nbTickers = 0)
