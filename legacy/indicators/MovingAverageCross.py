import sys
sys.path.insert(0, '..')

import numpy as np
import pandas as pd
from indicators.Indicator import Indicator
from data import ploter

class MovingAverageCross(Indicator):

    def __init__(self, window):
        self.window = window

    def compute(self, df, plot=False):
        frame = pd.DataFrame()

        frame['ma'] = df.groupby('asset')['close'].rolling(window=self.window,center=False).mean()
        frame.reset_index(level=0,drop=True,inplace=True)
        frame.sort_index(inplace=True)
        frame['shifted_open'] = df.groupby('asset')['open'].shift()
        frame['shifted_close'] = df.groupby('asset')['close'].shift()
        frame['shifted_ma'] = frame.groupby('asset')['ma'].shift()

        frame.dropna(inplace=True)
        frame['signal'] = frame.apply(lambda row: ((row.shifted_close - row.shifted_ma)/(row.shifted_ma - row.shifted_open))
                                    if row.shifted_close > row.shifted_ma and row.shifted_open < row.shifted_ma
                                    else 0, axis = 1)
        frame['signal'] = frame.apply(lambda row: -((row.shifted_close - row.shifted_ma)/(row.shifted_ma - row.shifted_open))
                                    if row.shifted_close < row.shifted_ma and row.shifted_open > row.shifted_ma
                                    else row.signal, axis = 1)

        if plot:
            ploter.ohlcv(df,['ma'])

        frame.drop(['ma','shifted_open','shifted_close','shifted_ma'],axis=1, inplace = True)
        return frame
