import unittest
from datahandler.bokstathandler import BokStatHandler
from tests.basetest import BaseTest
import os 
import json 

class BokStatTest(BaseTest):

    def setUp(self):
        super().setUp()
    #     self.bok_item_code = self.db.bok_item_code
    #     self.bok_stats = self.db.bok_stats
    #     self.bsh = BokStatHandler('testdb')
    #     self.item_data = self.prepare_item_data()
    #     self.bok_item_code.insert_many(self.item_data)


    # def prepare_item_data(self):
    #     with open('datacollections/test_item_code.json') as f:
    #         sample_list = json.load(f)
    #     return sample_list


    # def test_insert_stats(self):
    #     item_code_list = [ 
    #         {'ITEM_CODE':item['ITEM_CODE'],'STAT_CODE':item['STAT_CODE']} 
    #         for item in self.item_data
    #     ]
    #     self.bsh.insert_stat_list(item_code_list)
    #     for item in self.item_data:
    #         matched_items = list(self.bok_stats.find({'ITEM_CODE1':item['ITEM_CODE']}))
    #         self.assertEqual(matched_items[0]["ORIGINAL_ITEM_CODE"],item['ITEM_CODE'])
    #         self.assertTrue(len(matched_items) >= item['DATA_CNT'])
        

