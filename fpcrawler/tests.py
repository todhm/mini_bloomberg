import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from tests.test_api import *

if __name__ == "__main__":
    unittest.main()