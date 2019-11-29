import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest

from tests.bokstattest import BokStatTest
from tests.quandldatatest import QuandlDataTest
from tests.korean_data_test import StockDataHandlerTest,FinanceTest
from tests.utility_test import UtilityTest

if __name__ == "__main__":
    unittest.main()