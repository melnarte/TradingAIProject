import sys
sys.path.insert(0, './alphalens/')

import matplotlib
import numpy as np
import os
import pandas as pd
import ray

from .alphalens import alphalens
from .factors.compute import compute
from .parser import Parser
from .utils.decorators import useray


@ray.remote
def compute_factor_data(pricings, factor, period, quantiles):
    try :
        return alphalens.utils.get_clean_factor_and_forward_returns(factor, pricings, quantiles=quantiles, periods=[period])
    except alphalens.utils.MaxLossExceededError:
        return None

@ray.remote
def compute_alpha_beta(factor_data, demeaned=True, group_adjust=False):
    if factor_data is not None:
        return alphalens.performance.factor_alpha_beta(factor_data, demeaned=True, group_adjust=False)
    else:
        raise TypeError('Input factor_data is None.')

@ray.remote
def save_return_teer_sheet(factor_data, path):
    alphalens.tears.create_returns_tear_sheet(factor_data, save_path=path)

def create_dir(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


class AlphaFrame():

    def create_expression_dict(self):
        raise NotImplementedError('create_expression_dict method must be implemented and return a {name:expression_str} dict.')

    def load_data(self):
        raise NotImplementedError('load_data method must be implemented. Returns a historical data pandas dataframe with indexes (date,asset).')

    def load_pricings(self):
        raise NotImplementedError('load_pricings method must be implemented. Returns historical pricings data dataframe with date index.')

    def check_data_integrity(self):
        raise NotImplementedError("TO DO")

    def __init__(self, period, quantiles=5):
        expressions = self.get_expressions()
        factor_data = self.get_factor_data(expressions)
        print(factor_data.unstack())
        pricings = self.load_pricings()
        self.create_clean_factor_data(factor_data, pricings, period, quantiles)

    def get_expressions(self):
        exp_dict = self.create_expression_dict()
        parser = Parser()
        parser.add(exp_dict)
        return parser.parse()

    # Computing factor data in a separate function so full_data is garbage collected asap
    def get_factor_data(self, expressions):
        full_data = self.load_data()
        return compute(expressions, full_data)

    @useray
    def create_clean_factor_data(self, factor_data, pricings, period, quantiles=5):
        process = []
        names=[]
        factors = []
        keys = []

        for col, data in factor_data.iteritems():
            names.append(col)
            process.append(compute_factor_data.remote(pricings, data, period, quantiles))

        for p,n in zip(process, names):
            f = ray.get(p)
            if f is not None:
                keys.append(n)
                factors.append(f)

        self.clean_factors = dict(zip(keys,factors))

    @useray
    def get_alpha_factors(self):
        process = []
        names =[]
        for col, data in self.clean_factors.items():
            names.append(col)
            process.append(compute_alpha_beta.remote(data))

        alpha = []
        first=True
        for p in process:
            table = ray.get(p)
            if first:
                alpha = table
                first = False
            else:
                alpha = np.concatenate((alpha,table), axis=1)
        alpha = np.concatenate(([names],alpha), axis=0)
        return alpha

    @useray
    def save_returns_tear_sheets(self, path):
        matplotlib.use('Agg')
        matplotlib.pyplot.style.use('ggplot')
        dir = os.getcwd() + '/' + path

        process=[]
        for col, data in self.clean_factors.items():
            factor_dir = dir + '/' + col
            create_dir(factor_dir)
            process.append(save_return_teer_sheet.remote(data,factor_dir))
        ray.wait(process, num_returns=len(self.clean_factors))
