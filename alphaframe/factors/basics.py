import sys
sys.path.insert(0, '..')

from .factors import Factor, FixedFactor, TimeSeriesFactor, SimpleFactor
import numpy as np
import pandas as pd
import re
from ..utils.decorators import *

class Sum(SimpleFactor):

    @checkinputlisttypes([[pd.Series,Factor,int,float],[pd.Series,Factor,int,float]])
    def expression(self, inputs=[]):
        return inputs[0] + inputs[1]

class Substract(SimpleFactor):

    @checkinputlisttypes([[pd.Series,Factor,int,float],[pd.Series,Factor,int,float]])
    def expression(self, inputs=[]):
        return inputs[0] - inputs[1]

class Multiply(SimpleFactor):

    @checkinputlisttypes([[pd.Series,Factor,int,float],[pd.Series,Factor,int,float]])
    def expression(self, inputs=[]):
        return inputs[0] * inputs[1]

class Divide(SimpleFactor):

    @checkinputlisttypes([[pd.Series,Factor,int,float],[pd.Series,Factor,int,float]])
    def expression(self, inputs=[]):
        return inputs[0] / inputs[1]


class Returns(FixedFactor):

    @checkinputlisttypes()
    def __init__(self,inputs=[]):
        self.inputs = ['close']

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Areturns\(\)\Z',s)):
            return True,None
        else:
            return False,None

    @checkinputlisttypes([[pd.Series]])
    def expression(self,inputs=[]):
        if len(inputs) != 1:
            raise AttributeError("Returns factor takes 1 inputs but got " + str(len(inputs)))
        else:
            ustack = inputs[0].unstack()
            ret = ustack / ustack.shift() - 1
            return ret.stack()


class VWAP(FixedFactor):

    @checkinputlisttypes()
    def __init__(self,inputs=[]):
        self.inputs = ['average']

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Avwap\(\)\Z',s)):
            return True,None
        else:
            return False,None

    @checkinputlisttypes([[pd.Series]])
    def expression(self,inputs=[]):
        if len(inputs) != 1:
            raise AttributeError("WVAP factor takes 1 inputs but got " + str(len(inputs)))
        else:
            return inputs[0]


class ADV(Factor):

    @checkinputlisttypes([[int]])
    def __init__(self,inputs=[]):
        self.inputs = ['average', 'volume', inputs[0]]

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Aadv\(\d+\)\Z',s)):
            return True,s[4:-1]
        else:
            return False,None

    def __str__(self):
        name = type(self).__name__
        return name+'('+str(self.inputs[2])+')'

    @checkinputlisttypes([[pd.Series],[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = (inputs[0].unstack() * inputs[1].unstack()).rolling(window=inputs[2],center=False).mean()
        return ret.stack()


class Corr(Factor):

    @checkinputlisttypes([[str,Factor],[str,Factor],[int]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Acorr\(.+,.+,\d+\)\Z',s)):
            return True,s[5:-1]
        else:
            return False,None

    def __str__(self):
        return 'Corr(' + str(self.inputs[0]) + ', ' + str(self.inputs[1]) + ', ' + str(self.inputs[2]) + ')'

    @checkinputlisttypes([[pd.Series, Factor],[pd.Series, Factor],[int]])
    def expression(self, inputs=[]):
        one = inputs[0].unstack()
        two = inputs[1].unstack()
        print(inputs[0].unstack().head(10))
        print(inputs[1].unstack().head(10))
        ret = one.rolling(window=inputs[2],center=False).corr(two)
        ret.replace(-np.inf, 0, inplace=True)
        ret.replace(np.inf, 0, inplace=True)
        return ret.stack()


class Cov(Factor):

    @checkinputlisttypes([[str],[str],[int]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Acov\(.+,.+,\d+\)\Z',s)):
            return True,s[4:-1]
        else:
            return False,None

    def __str__(self):
        return 'Cov(' + str(self.inputs[0]) + ', ' + str(self.inputs[1]) + ', ' + str(self.inputs[2]) + ')'

    @checkinputlisttypes([[pd.Series, Factor],[pd.Series, Factor],[int]])
    def expression(self, inputs=[]):
        one = inputs[0].unstack()
        two = inputs[1].unstack()
        ret = one.rolling(window=inputs[2],center=False).cov(two)
        ret.replace(-np.inf, 0, inplace=True)
        ret.replace(np.inf, 0, inplace=True)
        ret.dropna(inplace=True)
        return ret.stack()


class Rank(Factor):

    @checkinputlisttypes([[str,Factor]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Arank\(.+\)\Z',s)):
            return True,s[5:-1]
        else:
            return False,None

    def __str__(self):
        return 'Rank(' + str(self.inputs[0]) + ')'

    @checkinputlisttypes([[pd.Series]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.rank(method='min', axis=1)
        return ret.stack()


class Delay(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Adelay\(.+,.+\)\Z',s)):
            return True,s[6:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.shift(inputs[1])
        return ret.stack()


class Power(Factor):

    @checkinputlisttypes([[str,Factor,int,float],[int,float]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Apower\(.+,.+\)\Z',s)):
            return True,s[6:-1]
        else:
            return False,None

    def __str__(self):
        return 'Power(' + str(self.inputs[0]) + ', ' + str(self.inputs[1]) + ')'

    @checkinputlisttypes([[pd.Series,Factor,int,float],[int,float]])
    def expression(self, inputs=[]):
        return inputs[0] ** inputs[1]


class Scale(Factor):

    @checkinputlisttypes([[str,Factor],[int,float]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ascale\(.+,.+\)\Z',s)):
            return True,s[6:-1]
        else:
            return False,None

    def __str__(self):
        return 'Scale(' + str(self.inputs[0]) + ', ' + str(self.inputs[1]) + ')'

    @checkinputlisttypes([[pd.Series],[int,float]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        total = ret.abs().sum(axis=1)
        ret = ret.divide(total, axis=0)
        ret = ret * inputs[1]
        return ret.stack()


class Delta(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Adelta\(.+,.+\)\Z',s)):
            return True,s[6:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret - ret.shift(inputs[1])
        return ret.stack()


class DecayLinear(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Adecay_linear\(.+,.+\)\Z',s)):
            return True,s[13:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        list = [i for i in range(1,inputs[1]+1)]
        su = sum(list)
        w = [i/su for i in list]
        ret = ret.rolling(window=inputs[1],center=False).apply(lambda x: np.average(x, weights=w, axis=0))
        return ret.stack()


class TsStddev(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ats_stddev\(.+,.+\)\Z',s)):
            return True,s[10:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.rolling(window=inputs[1],center=False).std()
        return ret.stack()


class TsProduct(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ats_product\(.+,.+\)\Z',s)):
            return True,s[11:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.rolling(window=inputs[1],center=False).apply(np.prod)
        return ret.stack()


class TsSum(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ats_sum\(.+,.+\)\Z',s)):
            return True,s[7:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.rolling(window=inputs[1],center=False).sum()
        return ret.stack()


class TsMax(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ats_max\(.+,.+\)\Z',s)):
            return True,s[7:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.rolling(window=inputs[1],center=False).max()
        return ret.stack()


class TsMin(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ats_min\(.+,.+\)\Z',s)):
            return True,s[7:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = pd.rolling_min(ret,inputs[1])
        return ret.stack()


class TsRank(TimeSeriesFactor):

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Ats_rank\(.+,.+\)\Z',s)):
            return True,s[8:-1]
        else:
            return False,None

    @checkinputlisttypes([[pd.Series],[int]])
    def expression(self, inputs=[]):
        ret = inputs[0].unstack()
        ret = ret.rolling(window=inputs[1],center=False).mean().rank(axis=1)
        return ret.stack()


class Log(Factor):

    @checkinputlisttypes([[str,Factor,int,float]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Alog\(.+\)\Z',s)):
            return True,s[4:-1]
        else:
            return False,None

    def __str__(self):
        name = type(self).__name__
        return name+'('+str(self.inputs[0])+')'

    @checkinputlisttypes([[pd.Series,int,float]])
    def expression(self, inputs=[]):
        return np.log10(inputs[0])


class Abs(Factor):

    @checkinputlisttypes([[str,Factor,int,float]])
    def __init__(self,inputs=[]):
        self.inputs = inputs

    @staticmethod
    def match(s):
        if type(s) != str:
            raise TypeError('match method takes string attribute but got ' + str(type(s)))
        if(re.match(r'\Aabs\(.+\)\Z',s)):
            return True,s[4:-1]
        else:
            return False,None

    def __str__(self):
        name = type(self).__name__
        return name+'('+str(self.inputs[0])+')'

    @checkinputlisttypes([[pd.Series,int,float]])
    def expression(self, inputs=[]):
        return np.fabs(inputs[0])
