import json
from random import randint
from datahandler import marketdatahandler
from datetime import datetime as dt
from .basetest import BaseTest

    
class MarketTest(BaseTest):
    def setUp(self):
        super().setUp()
        self.mdh = marketdatahandler.MarketDataHandler(dbName='testdb')
        
    def tearDown(self):
        super().tearDown()

    def test_market_mongo_insert(self): 
        self.mdh.prepare_marcap_data(1995, 1996)
        result = list(self.db.market_data.find({'Code': '015760'}))
        self.assertTrue(len(result) >= 450)

    def test_calling_api_market(self):
        startdate = dt.strptime('20200501', '%Y%m%d')
        enddate = dt.strptime('20200531', '%Y%m%d')
        self.mdh.save_api_market_data(startdate, enddate)
        result = list(self.db.market_data.find())
        self.assertTrue(len(result) >= 2050)

