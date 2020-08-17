import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from tests.test_market import * 
from tests.report_data_tests import *


if __name__ == "__main__":
    unittest.main()