import sys
sys.path.insert(0, '..')

from indicators.Indicator import Indicator
import numpy as np
import pandas as pd
from utils import factors_utils


class One(Indicator):

    def __init__(self):
        return

    #(rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) -0.5)
    def compute(self, df):
        frame = pd.DataFrame()
        frame['returns'] = factors_utils.returns(df)
        frame['stddev'] = factors_utils.ts_stddev(frame.returns, 20)
        frame['close'] = df.close
        frame.dropna(inplace=True)
        frame['ifretn'] = frame.apply(lambda row : row.stddev if row.returns < 0 else row.close, axis = 1)
        frame['power'] = frame.ifretn**2
        frame['signal'] = factors_utils.rank(factors_utils.ts_max(frame.power,5)) - 0.5
        print(frame.head(10))
        frame.drop(['returns', 'stddev', 'close','ifretn','power'], axis=1, inplace=True)
        frame.dropna(inplace=True)
        return frame


class Two(Indicator):

    def __init__(self):
        self.fields = ['open','close','volume']
        return

    #(-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
    def compute(self, df):
        frame = pd.DataFrame()
        frame['lv'] = df.apply(lambda row : np.log(row.volume) if row.volume > 1 else 0, axis=1)
        frame['dr'] = df.apply(lambda row : row.close/row.open -1 if row.open != row.close else 0, axis=1)
        frame['first'] = factors_utils.rank(factors_utils.delta(frame['lv'],2))
        frame['second'] = factors_utils.ts_rank( frame['dr'], 6 )
        frame['signal'] = -1 * factors_utils.correlation(frame['first'], frame['second'], 6)
        frame.drop(['lv','dr','first','second'], axis=1, inplace=True)
        frame.dropna(inplace=True)
        frame = frame.unstack()
        frame.fillna(value=0, inplace=True)
        frame = frame.stack()
        return frame


# class Three(Indicator):
#
#     def __init__(self):
#         return
#
#     #(-1 * correlation(rank(open), rank(volume), 10))
#     def compute(self, df):
#         frame = pd.DataFrame()
#         frame['first'] = factors_utils.rank(df.open)
#         frame['second'] = factors_utils.rank(df.volume)
#         print(frame.unstack().head(10))
#         frame['signal'] = -1 * factors_utils.correlation(frame['first'], frame['first'], 10)
#         frame.drop(['first', 'second'], axis = 1, inplace = True)
#         return frame


class Four(Indicator):

    def __init__(self):
        self.fields = ['low']
        return

    #(-1 * Ts_Rank(rank(low), 9))
    def compute(self, df):
        frame = pd.DataFrame()
        frame['signal'] = factors_utils.ts_rank(factors_utils.rank(df.low), 9) * -1
        return frame

# class HundredOne(Indicator):
#
#     def __init__(self):
#         return
#
#     # ((close - open) / ((high - low) + .001))
#     def compute(self, df):
#         frame = pd.DataFrame()
#         frame['signal'] = (df.groupby('asset')['close'].shift() - df.groupby('asset')['open'].shift())/(df.groupby('asset')['high'].shift() - df.groupby('asset')['low'].shift())+0.001
#         frame.dropna(inplace=True)
#         return frame
