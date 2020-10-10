import datetime as dt
import requests


class Collector():

    def __init__(self, data_path : str, api_token='pk_d4de3407f29a4f7897c3669c42f11a61'):
        self.data_path = data_path
        self.api_token = api_token

    # Retreive pricing data in 1 minute samples for a specific ticker and date from IEXCloud API
    def get_1d_minute_data(self,ticker,date):
        dateStr = date.strftime('%Y%m%d')
        url = 'https://cloud.iexapis.com/stable/stock/' + ticker + '/intraday-prices?token=' + self.api_token + "&chartIEXOnly=true&exactDate=" + dateStr
        r = requests.get(url)
        data = r.json()
        return data
