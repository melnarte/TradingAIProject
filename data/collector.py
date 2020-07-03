import sys
sys.path.insert(0, '..')

import bs4 as bs
import csv
from csv import reader
import datetime as dt
import pandas as pd
import requests
import time
from data.load import load_ticker_list, merge_data_to_file
from alphaframe.utils import date_utils



class DataCollector:

    def __init__(self, api_token='pk_d4de3407f29a4f7897c3669c42f11a61', data_path = '../../data/stocks/'):
        self.data_path = data_path
        self.intraday_api_url = 'https://cloud.iexapis.com/stable/stock/'
        self.search_api_url = 'https://cloud.iexapis.com/stable/search/'
        self.api_token = api_token


    # Retreive data for tickers of the S&P500 from IEXCloud
    def get_snp500_data(self, start_date, end_date, fromIndex=0, nbTickers=1):
        nb_of_tickers = 0
        nb_data_points = 0
        nb_no_data_days = 0

        start = date_utils.to_date(start_date)
        end = date_utils.to_date(end_date)

        tickers = load_ticker_list(self.data_path + 'snp500_tickers.csv')
        for i in range(fromIndex, fromIndex+nbTickers):
            print("Getting " + tickers[i] + "...")
            data_p, no_data = self.get_ticker_data(tickers[i], start, end)
            nb_of_tickers += 1
            nb_data_points += data_p
            nb_no_data_days += no_data

        print("Finished getting data for S&P500 from IEXCloud")
        print("Retreived " + str(nb_data_points) + " from " + str(nb_of_tickers) + " stocks over " + str(nbDays) + " days.")
        print("There was a total of " + str(nb_no_data_days) + " days with missing data over the entire data collection.")


    # Retreived data for a specific ticker over a time period from IEXCloud
    def get_ticker_data(self, ticker, start_date, end_date):
        nb_no_data_days = 0
        nb_data_points = 0

        start = date_utils.to_date(start_date)
        end = date_utils.to_date(end_date)

        delta = dt.timedelta(days=1)
        date = start

        while(date != end + delta):
            if(date.weekday() < 5):
                df = self.get_1d_minute_data(ticker, date)
                if(df.size != 0):
                    merge_data_to_file(self.data_path + ticker + '.csv',df)
                    nb_data_points += df.size
                else:
                    nb_no_data_days += 1
            date += delta

        return nb_data_points, nb_no_data_days


    # Retreive pricing data in 1 minute samples for a specific ticker and date from IEXCloud
    def get_1d_minute_data(self,ticker,date):
        dateStr = date.strftime('%Y%m%d')
        url = self.intraday_api_url + ticker + '/intraday-prices?token=' + self.api_token + "&chartIEXOnly=true&exactDate=" + dateStr
        r = requests.get(url)
        print("\t" + date.strftime('%Y/%m/%d') + ' : ' + str(r))
        data = r.json()
        df = pd.DataFrame.from_dict(data)
        if(df.size != 0):
            df.set_index('date', inplace=True)
        return df


    # Retreive the S&P500 tickers from wikipedia and saves them to file
    def get_snp500_tickers(self):
        resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class':'wikitable sortable'})
        tickers = []

        for row in table.findAll('tr')[1:]:
            ticker = row.findAll('td')[0].text[:-1]
            tickers.append(ticker)

        tickers = list(dict.fromkeys(tickers))
        tickers.sort()

        with open(self.data_path + "snp500_tickers.csv", 'w', newline='') as f:
            wr = csv.writer(f, delimiter='\n')
            wr.writerow(tickers)
