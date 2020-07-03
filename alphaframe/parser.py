from .factors.basics import *
from .utils.decorators import useray
import pandas as pd
import re
import ray


class Parser():

    def __init__(self, columns=[]):
        if type(columns) != list:
            raise TypeError("Columns must be a list or None.")
        if len(columns) != 0 and type(columns[0]) != str:
            raise ValueError("Colmuns argument must be a list of strings.")

        self.strings = {}
        self.factors = {'returns':Returns,
                        'delay':Delay,
                        'vwap' :VWAP,
                        'adv' :ADV,
                        'rank' :Rank,
                        'corr' :Corr,
                        'cov' :Cov,
                        'scale' :Scale,
                        'delta' :Delta,
                        'power' :Power,
                        'decay_linear' :DecayLinear,
                        'ts_stddev' : TsStddev,
                        'ts_product' : TsProduct,
                        'ts_sum' : TsSum,
                        'ts_max' : TsMax,
                        'ts_min' : TsMin,
                        'ts_rank' : TsRank,
                        'log' : Log,
                        'abs' : Abs}
        if len(columns) == 0:
            self.columns = ['open','high','low','close','volume','notional','numberOfTrades','average']
        else:
            self.columns = columns

    def add(self, strings, name = None):
        if type(strings) not in [str,dict]:
            raise TypeError("Imput must be a string or dict<name:expressions_string>")
        if type(strings) is str:
            if name is None:
                raise ValueError("If passing a single expression string, a name muste be passed as well.")
            elif type(name) is not str:
                raise TypeError('Name must be a string')
            else:
                self.strings.update({name:strings})
        if type(strings) is dict:
            self.strings.update(strings)


    # Parse the expressions, returns a dict of {name:expression}
    @useray
    def parse(self):

        @ray.remote
        def parse_expression(key, string):
            return {key : self.parse_str(string)}

        @ray.remote
        def concat_dict(dict1,dict2):
            dict1.update(dict2)
            return dict1

        expressions = []
        for k, s in self.strings.items():
            expressions.append(parse_expression.remote(k,s))
            if len(expressions)>3:
                ray.wait(expressions)
        cnt=0
        while len(expressions) > 1:
            expressions = expressions[2:] + [concat_dict.remote(expressions[0], expressions[1])]
            if cnt>3:
                ray.wait(expressions)
            else:
                cnt+=1
        return ray.get(expressions[0])


    def match_int(self,s):
        if re.match(r'\A\d+\Z',s):
            return 1
        elif re.match(r'\A-\d+\Z',s):
            return -1
        else:
            return 0

    def match_float(self, s):
        if re.match(r'\A\d+[\.,]\d*\Z',s):
            return 1
        elif re.match(r'\A-\d+[\.,]\d*\Z',s):
            return -1
        else:
            return 0

    # Match columns name from self.columns
    def match_column_name(self,s):
        for c in self.columns:
            if(re.match(r'\A'+c+r'\Z', s)):
                return c
        return None

    # Match and parse factors from self.factors dict
    def match_factor(self,s):
        for f in self.factors.keys():
            match, args =  self.factors[f].match(s)
            if self.parentheses_enclosed(s) and match:
                if args is not None:
                    return self.factors[f](self.parse_args(args))
                else:
                    return self.factors[f]([])
        return None

    # Match and parse +,-,*,/
    def match_basic_operations(self, s):
        if re.match(r'.*[-+*/].+',s):
            rest_s = self.remove_match_parentheses(s)
            if '+' in rest_s or '-' in rest_s: # Spliting for + and -
                comps = re.compile(r'[+-]').split(s)
            elif '*' in rest_s or '/' in rest_s: # Splitting for * and /
                comps = re.compile(r'[*/]').split(s)
            else:
                return None


            # If there is a - at the beginning of the string
            if len(comps[0]) == 0 and s[0] == '-':
                return Multiply([-1, self.parse_str(s[1:])])

            # Getting position of fist operator outside of parenthesis
            pos = self.first_not_nested_operator_pos(comps)

            left = s[:pos]
            right = s[pos+1:]
            op = s[pos]

            if op == '+':
                return Sum([self.parse_str(left), self.parse_str(right)])
            elif op == '-':
                return Substract([self.parse_str(left), self.parse_str(right)])
            elif op == '*':
                return Multiply([self.parse_str(left), self.parse_str(right)])
            elif op == '/':
                return Divide([self.parse_str(left), self.parse_str(right)])
            else:
                return None
        else:
            return None

    # Parse an expression string
    def parse_str(self, s):
        if type(s) is not str:
            raise TypeError('Parser input must be str, but ' + str(type(s)) + 'were given')

        if len(s) == 0:
            raise ValueError('Parser got empty string')

        s = s.replace(' ','')

        coef = self.match_int(s)
        if coef == 1:
            return int(s)
        elif coef == -1:
            return -int(s[1:])

        coef = self.match_float(s)
        if coef == 1:
            return float(s)
        elif coef == -1:
            return -float(s[1:])

        # Stripping parentheses from enclosed expressions:
        if re.match(r'\A\(.+\)\Z',s) and self.parentheses_enclosed(s):
            return self.parse_str(s[1:-1])

        col = self.match_column_name(s)
        if col is not None:
            return col

        factor = self.match_factor(s)
        if factor is not None:
            return factor

        op = self.match_basic_operations(s)
        if op is not None:
            return op

        raise ValueError("Unable to match %s"%(s))


    # Parse the arguments args
    def parse_args(self, args):
        ripped_args = self.remove_match_parentheses(args)
        if ',' in ripped_args:
            comps = args.split(',')
            i=0
            while i < len(comps):
                while(comps[i].count('(') != comps[i].count(')')):
                    comps[i:i+2] = [comps[i]+','+comps[i+1]]
                i +=1
            ret=[]
            for arg in comps:
                ret.append(self.parse_str(arg))
            return ret
        else:
            return [self.parse_str(args)]

    # Remove all match parentheses in s
    def remove_match_parentheses(self, s):
        if ')' in s:
            end = s.find(')')
            if '(' in s[:end]:
                start = max([i for i, char in enumerate(s[:end]) if char == '('])
                return self.remove_match_parentheses(s[:start]+s[end+1:])
            else:
                return s[:end+1] + self.remove_match_parentheses(s[end+1:])
        else:
            return s

    # True is the expression is enclose, eg. (some_stuff)
    def parentheses_enclosed(self, s):
        paren_order = re.findall(r'[\(\)]', s)

        if paren_order.count('(') != paren_order.count(')'):
            return False

        curr_levels = []
        nest_lv = 0
        for p in paren_order:
            if p == '(':
                nest_lv += 1
            else:
                nest_lv -= 1
            curr_levels.append(nest_lv)
        if 0 in curr_levels[:-1]:
            return False
        else:
            return True

    # Returns the position of the first comps end that is not inside a parentheses
    def first_not_nested_operator_pos(self, comps):
        if comps[0].count('(') != comps[0].count(')'):
            nested_level = comps[0].count('(') - comps[0].count(')')
            pos = len(comps[0])
            for comp in comps[1:]:
                if '(' in comp:
                    nested_level += comp.count('(')
                if ')' in comp:
                    nested_level -= comp.count(')')
                pos += len(comp) + 1
                if nested_level == 0:
                    return pos
        else:
            return len(comps[0])
