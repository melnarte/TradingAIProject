import sys
sys.path.insert(0, '.')

from alphaframe.factors.basics import *
import pandas as pd
import unittest


class FactorTest(unittest.TestCase):

    def test_sum(self):
        f = pd.DataFrame()
        self.assertEqual(Sum([4,5]).compute(f),9)
        return


if __name__=='__main__':
    unittest.main()
