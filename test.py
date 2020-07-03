from alphaframe.alphaFrame import AlphaFrame
from data import load
from alphaframe.parser import Parser
from alphaframe.utils import date_utils
from alphaframe.factors.compute import compute
import pandas as pd

class Test(AlphaFrame):
    def create_expression_dict(self):
        dict =  {'four': 'ts_rank(rank(low), 9)',
                 'five': '-(rank(open - (ts_sum(vwap(), 10) / 10)) * (-abs(rank((close - vwap())))))'}
        return dict

    def load_data(self):
        start_date, end_date = date_utils.get_clean_trading_dates('2019-12-29', '2019-12-31')
        return load.load_tickers('stock_data/stocks/', load.load_ticker_list('stock_data/stocks/snp500_tickers.csv'), start_date, end_date)

    def load_pricings(self):
        start_date, end_date = date_utils.get_clean_trading_dates('2019-12-29', '2019-12-31')
        return load.get_pricings('stock_data/stocks/', load.load_ticker_list('stock_data/stocks/snp500_tickers.csv'), start_date, end_date)

a = Test(10)
print(a.get_alpha_factors())
a.save_returns_tear_sheets('./test_path')
