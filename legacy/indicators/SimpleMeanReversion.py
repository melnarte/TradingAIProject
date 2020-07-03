import sys
sys.path.insert(0, '..')

from indicators.Indicator import Indicator
import numpy as np
import pandas as pd


class SimpleMeanReversion(Indicator):

    def __init__(self):
        return

    def compute(self, df):
        frame = pd.DataFrame()
        frame['signal'] = - np.log(df['open'] / df.groupby('asset')['close'].shift())
        return frame
