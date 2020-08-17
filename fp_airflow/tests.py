import os, sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tests.reporttest import *


if __name__ == "__main__":
    unittest.main()