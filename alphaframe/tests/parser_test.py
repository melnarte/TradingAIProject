import sys
sys.path.insert(0, '.')

from alphaframe.factors.basics import Returns
from alphaframe import parser
import unittest


class ParserTest(unittest.TestCase):

    def test_parse_str(self):
        p = parser.Parser()
        exp1 = p.parse_str('returns()')
        exp2 = p.parse_str('delay(open+close,4)')
        exp3 = p.parse_str('d elay (open + ( close-  5),2)-retur  ns()')
        exp4 = p.parse_str('open+close')
        exp5 = p.parse_str('open-close')
        exp6 = p.parse_str('volume*numberOfTrades')
        exp7 = p.parse_str('high/low')
        exp8 = p.parse_str('vwap()')
        exp9 = p.parse_str('adv(5)')
        exp10 = p.parse_str('corr(low,notional,3)')
        exp11 = p.parse_str('cov(low,notional,3)')
        exp12 = p.parse_str('rank(close+open)')
        exp13 = p.parse_str('power(close,3)')
        exp14 = p.parse_str('scale(high,5.7)')
        exp15 = p.parse_str('delta(low,5)')
        exp16 = p.parse_str('decay_linear(high,7)')
        exp17 = p.parse_str('ts_stddev(average,2)')
        exp18 = p.parse_str('ts_product(numberOfTrades,6)')
        exp19 = p.parse_str('ts_sum(open,5)')
        exp20 = p.parse_str('ts_max(open,2)')
        exp21 = p.parse_str('ts_min(close,4)')
        exp22 = p.parse_str('ts_rank(open,7)')

        self.assertEqual(str(exp1), 'Returns()')
        self.assertEqual(str(exp2), 'Delay(Sum(open, close), 4)')
        self.assertEqual(str(exp3), 'Substract(Delay(Sum(open, Substract(close, 5)), 2), Returns())')
        self.assertEqual(str(exp4), 'Sum(open, close)')
        self.assertEqual(str(exp5), 'Substract(open, close)')
        self.assertEqual(str(exp6), 'Multiply(volume, numberOfTrades)')
        self.assertEqual(str(exp7), 'Divide(high, low)')
        self.assertEqual(str(exp8), 'VWAP()')
        self.assertEqual(str(exp9), 'ADV(5)')
        self.assertEqual(str(exp10), 'Corr(low, notional, 3)')
        self.assertEqual(str(exp11), 'Cov(low, notional, 3)')
        self.assertEqual(str(exp12), 'Rank(Sum(close, open))')
        self.assertEqual(str(exp13), 'Power(close, 3)')
        self.assertEqual(str(exp14), 'Scale(high, 5.7)')
        self.assertEqual(str(exp15), 'Delta(low, 5)')
        self.assertEqual(str(exp16), 'DecayLinear(high, 7)')
        self.assertEqual(str(exp17), 'TsStddev(average, 2)')
        self.assertEqual(str(exp18), 'TsProduct(numberOfTrades, 6)')
        self.assertEqual(str(exp19), 'TsSum(open, 5)')
        self.assertEqual(str(exp20), 'TsMax(open, 2)')
        self.assertEqual(str(exp21), 'TsMin(close, 4)')
        self.assertEqual(str(exp22), 'TsRank(open, 7)')

    def test_type(self):
        p = parser.Parser()

        inputs = [2,0,Returns([])]
        for i in inputs:
            self.assertRaises(TypeError, p.parse, i)

    def test_value(self):
        p = parser.Parser()
        inputs = ['', ' ', 'return()', '15,4', '()']
        for i in inputs:
            self.assertRaises(ValueError, p.parse_str, i)

    def test_match_int(self):
        p = parser.Parser()
        inputs=['1', '25', '0', '1016856'
        ]
        for i in inputs:
            self.assertEqual(p.match_int(i),1)

        inputs=['-1', '-25', '-6', '-12300']
        for i in inputs:
            self.assertEqual(p.match_int(i),-1)

        inputs=['-jhvxc1', '', '()', '*8', 'text','...','8.5478','-5.68','4,56','-58.69']
        for i in inputs:
            self.assertEqual(p.match_int(i),0)

    def test_match_float(self):
        p = parser.Parser()

        inputs=['1', '25', '0', '1016856','-jhvxc1', '', '()', '*8', 'text','...','-1', '-25', '-6', '-12300']
        for i in inputs:
            self.assertEqual(p.match_float(i),0)

        inputs=['-5.485','-9.46687','-1654.3']
        for i in inputs:
            self.assertEqual(p.match_float(i),-1)

        inputs=['8.5478','5.68','4,56','58.69','0.0']
        for i in inputs:
            self.assertEqual(p.match_float(i),1)

    def test_match_column_name(self):
        p = parser.Parser()

        inputs=['open','high','low','close','volume','numberOfTrades','notional','average']
        for i in inputs:
            self.assertIsNotNone(p.match_column_name(i))

        inputs=['',' ','+open','open-','op en','jkgfyu','clos','closee','5546','text']
        for i in inputs:
            self.assertIsNone(p.match_column_name(i))

    def test_basic_operations(self):
        p = parser.Parser()

        inputs=['5+6.5','4-2','-open','8/3','8*7','-1','-(5*7+6)+(8*(4/5))']
        for i in inputs:
            self.assertIsNotNone(p.match_basic_operations(i))

        inputs=['(5*4)','open','text','returns()','corr(open-close,close,2)']
        for i in inputs:
            self.assertIsNone(p.match_basic_operations(i))

    def test_remove_matched_parenthesis(self):
        p = parser.Parser()

        inputs=[['5','4*(6-4)','(8/4)*(4-(6+7))','()','','(',')','4)*(4+2)','(4*(5)'],
                ['5','4*',     '*',              '', '',  '(',')','4)*','(4*']]

        for i in range(len(inputs[0])):
            self.assertEqual(p.remove_match_parentheses(inputs[0][i]), inputs[1][i])

    def test_parenthesis_enclosed(self):
        p = parser.Parser()

        inputs = ['()','(5*4)','(5*4+(-5+3))']
        for i in inputs:
            self.assertTrue(p.parentheses_enclosed(i))

        inputs = ['(',')','(5*4','((5*4+(-5+3))+ 8*4','(5*4+(-5+3))+ 8*4)']
        for i in inputs:
            self.assertFalse(p.parentheses_enclosed(i))


if __name__=='__main__':
    unittest.main()
