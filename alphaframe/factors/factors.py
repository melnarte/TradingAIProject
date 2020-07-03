import sys
sys.path.insert(0, '..')

import inspect
import pandas as pd
from ..utils.decorators import *


class Factor():
    """
    virtual Factor class. Defines the formula for a factor that computes from prices.
    The factor takes an arbitrary number of inputs. Inputs are data_columns, scalars are other factors.
    The compute method return a column with the result of the factor computation.
    """
    def __init__(self, inputs):
        self.inputs = inputs

    @staticmethod
    def match(s):
        """
        Use regex to see if the string s is the factor
        args:
            s : str
            the expression to match

        returns:
            True, args of the factor if there is a match
            False, None else
        """
        raise NotImplementedError()

    def compute(self, df):
        if type(df) != pd.DataFrame:
            raise TypeError('Compute argument must be pandas DataFrame but '+ str(type(df)) + ' were given.')

        computed_inputs = []

        for input in self.inputs:

            if isinstance(input,Factor):
                computed_inputs.append(input.compute(df))

            elif isinstance(input,str):
                computed_inputs.append(df[input])

            else:
                computed_inputs.append(input)

        ret = self.expression(computed_inputs)
        return ret

    def expression(self, inputs=[]):
        raise NotImplementedError()

    def __str__(self):
        return NotImplementedError


class FixedFactor(Factor):
    def __str__(self):
        name = type(self).__name__
        return name+'()'


class TimeSeriesFactor(Factor):

    @checkinputlisttypes([[Factor,str],[int]])
    def __init__(self, inputs=[]):
        if len(inputs) != 2:
            raise ValueError(type(self).__name__ + ' : time series factor takes 2 inputs, ' + len(inputs) + 'were given')
        if inputs[1] < 0 or type(inputs[1]) != int:
            raise TypeError(type(self).__name__ + ' : time series second input must be a positive integer')
        self.inputs = inputs

    def __str__(self):
        name = type(self).__name__
        first = str(self.inputs[0])
        return name+'(' + first +', ' + str(self.inputs[1]) + ')'

class SimpleFactor(Factor):

    @checkinputlisttypes([[Factor,str,int,float],[Factor,str,int,float]])
    def __init__(self, inputs):
        self.inputs = inputs

    def __str__(self):
        return type(self).__name__ + '(' + str(self.inputs[0]) + ', ' + str(self.inputs[1]) + ')'
