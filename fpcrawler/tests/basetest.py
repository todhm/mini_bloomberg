import unittest
from pymongo import MongoClient
import os 

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.mongo_client = MongoClient(os.environ.get('MONGO_URI'))
        self.db = self.mongo_client.testdb



    def tearDown(self):
        self.mongo_client.drop_database('testdb')
        self.mongo_client.close()
        