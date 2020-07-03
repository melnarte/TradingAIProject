import numpy as np
import pandas as pd


def returns(df):
    frame = df.unstack()
    ret = frame.close / frame.close.shift() - 1
    return ret.stack()

def vwap(df):
    return df.average

def adv(df,d):
    frame = df.unstack()
    ret = (frame.average * frame.volume).rolling(window=d,center=False).mean()
    return ret.stack()

def rank(serie):
    s = serie.unstack()
    ret = s.rank(method='min', axis=1)
    return ret.stack()

def delay(serie,d):
    s = serie.unstack()
    ret = s.shift(d)
    return ret.stack()

def correlation(x,y,d):
    xu = x.unstack()
    yu = y.unstack()
    ret = xu.rolling(window=d,center=False).corr(yu)
    return ret.stack()

def covariance(x,y,d):
    xu = x.unstack()
    yu = y.unstack()
    ret = xu.rolling(window=d,center=False).cov(yu)
    return ret.stack()

def scale(serie, a):
    s = serie.unstack()
    total = s.abs().sum(axis=1)
    ret = s.divide(total, axis=0)
    return ret.stack()

def delta(serie, d):
    return serie - serie.shift(d)

def decay_linear(serie,d):
    s = serie.unstack()
    list = [i for i in range(1,d+1)]
    su = sum(list)
    w = [i/su for i in list]
    print(w)
    ret = s.rolling(window=d,center=False).apply(lambda x: np.average(x, weights=w, axis=0))
    return ret.stack()

def ts_stddev(serie,d):
    s = serie.unstack()
    ret = s.rolling(window=d,center=False).std()
    return ret.stack()

def ts_product(serie,d):
    s = serie.unstack()
    ret = s.rolling(window=d,center=False).apply(np.prod)
    return ret.stack()

def ts_sum(serie, d):
    s = serie.unstack()
    ret = s.rolling(window=d,center=False).sum()
    return ret.stack()

def ts_max(serie,d):
    s = serie.unstack()
    ret = pd.rolling_max(s,d)
    return ret.stack()

def ts_min(serie,d):
    s = serie.unstack()
    ret = pd.rolling_min(s,d)
    return ret.stack()

def ts_rank(serie,d):
    s = serie.unstack()
    ret = s.rolling(window=d,center=False).mean().rank(axis=1)
    return ret.stack()
