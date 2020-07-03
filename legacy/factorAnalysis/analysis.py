import sys
sys.path.insert(0, '..')

import alphalens
from data import load
import datetime as dt
from indicators import *
import matplotlib.pyplot as plt
import matplotlib
import multiprocessing
import os
import pandas as pd
import ray
import time
from utils import date_utils


@ray.remote
class DataServer():

    def __init__(self, data_path, tickers, start_date, end_date, quantiles, bins, periods=(1,5,10),fields='all'):
        self.data_path = data_path
        self.tickers = tickers
        self.start_date, self.end_date = date_utils.get_clean_trading_dates(start_date, end_date)
        self.periods = periods
        start = time.time()
        self.pricings, self.full_data = self.load_data(fields)
        end = time.time()
        print('Loading data : ' + str(end-start) + " secs.")


    # Load the data necessary to sompute the signal and perform the analysis
    def load_data(self, fields='all'):
        gpr = ray.remote(load.get_pricings)
        ltr = ray.remote(load.load_tickers)
        p = []
        p.append(gpr.remote(self.data_path+'/stocks/', self.tickers, self.start_date, self.end_date))
        p.append(ltr.remote(self.data_path+'/stocks/', self.tickers, self.start_date, self.end_date - dt.timedelta(minutes=self.periods[-1]+1), fields=fields))
        ray.wait(p, num_returns=2)
        return ray.get(p)


    # Returns the pricings
    def get_pricings(self):
        return self.pricings

    def get_data(self):
        return self.full_data

    def get_periods(self):
        return self.periods

    def get_dates(self):
        return [self.start_date, self.end_date]

    def get_bins(self):
        return self.bins

    def get_quantiles(self):
        return self.quantiles


@ray.remote
class DataAnalyzer():

    def __init__(self, indicator, dataServer):
        self.indicator = indicator
        self.ds = dataServer
        self.signal = self.create_factor_data()

    # Create clean factor data for alphalens
    def create_factor_data(self):
        start = time.time()
        signal = self.indicator.compute(ray.get(self.ds.get_data.remote()))
        # print("Factor computed")
        # factor_data = alphalens.utils.get_clean_factor_and_forward_returns(signal, ray.get(ds.pricings), quantiles = ray.get(ds.quantiles), bins=ray.get(ds.bins), periods=ray.get(ds.periods))
        # end = time.time()
        # print('Creating factor : ' + str(end-start) + " secs.")
        return signal

    def get_signal(self):
        return self.signal


class FactorAnalyser():

    def __init__(self, data_path, tickers, start_date, end_date, periods=(1,5,10),fields='all'):
        self.data_path = data_path
        self.tickers = tickers
        self.start_date, self.end_date = date_utils.get_clean_trading_dates(start_date, end_date)
        self.periods = periods
        self.load_data(fields)
        if (self.check_data_index_integrity() == False):
            print('ERROR : problem with data index')
        print("Data Loaded")


    # Load the data necessary to sompute the signal and perform the analysis
    def load_data(self, fields='all'):
        start = time.time()
        self.pricing =load.get_pricings(self.data_path+'/stocks/', self.tickers, self.start_date, self.end_date)
        self.full_data = load.load_tickers(self.data_path+'/stocks/', self.tickers, self.start_date, self.end_date - dt.timedelta(minutes=self.periods[-1]+1), fields=fields)
        end = time.time()
        print('Loading data : ' + str(end-start) + " secs.")


    # Check if the is no missing index in pricing with regard to full_data
    def check_data_index_integrity(self):
        start = time.time()
        full_data = self.full_data.unstack()
        if(self.pricing.index.difference(full_data.index).size != self.periods[-1]+1):
            return False
        end = time.time()
        print('Check data : ' + str(end-start) + " secs.")


    # Create clean factor data for alphalens
    def create_factor_data(self,indicator, quantiles, bins):
        start = time.time()
        self.signal = indicator.compute(self.full_data)
        print("Factor computed")
        factor_data = alphalens.utils.get_clean_factor_and_forward_returns(self.signal, self.pricing, quantiles = quantiles, bins=bins, periods=self.periods)
        end = time.time()
        print('Creating factor : ' + str(end-start) + " secs.")
        return factor_data


    # Save the full tear sheet to file
    def save_full_tear_sheet(self, data_path, dir_name, indicator, quantiles, bins):
        start = time.time()
        matplotlib.use('Agg')
        plt.style.use('ggplot')
        factor_data = self.create_factor_data(indicator, quantiles, bins)

        dir =os.getcwd() + '/' + data_path + '/factor_analysis/tear_sheet/' + dir_name + '_' + dt.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        os.makedirs(dir)

        info = ray.remote(alphalens.tears.create_information_tear_sheet)
        turnover = ray.remote(alphalens.tears.create_turnover_tear_sheet)
        event = ray.remote(alphalens.tears.create_event_study_tear_sheet)
        returns = ray.remote(alphalens.tears.create_returns_tear_sheet)
        processes =[]

        processes.append(info.remote(factor_data,save_path=dir))
        processes.append(turnover.remote(factor_data,save_path=dir))
        processes.append(returns.remote(factor_data,save_path=dir))
        alphalens.tears.create_event_study_tear_sheet(factor_data,save_path=dir)
        ray.wait(processes, num_returns=3)
        end = time.time()
        print('Tear sheet : ' + str(end-start) + " secs.")
