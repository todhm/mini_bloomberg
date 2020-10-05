import unittest
from pymongo import MongoClient
from datahandler.metadatahandler import MetaDataHandler
from datahandler.quandldatahandler import QuandlDataHandler
from tests.basetest import BaseTest
import os 


class QuandlDataTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.quandl_meta = self.db.quandl_meta
        self.quandl_stats = self.db.quandl_stats
        self.qdh = QuandlDataHandler('testdb')

        

    def test_insert_metadata(self):
        code_list = ['CHRIS/CME_RB1','CHRIS/CME_CL1']
        columns_data = [
            'id',
            'dataset_code',
            'database_code',
            'name',
            'description',
            'refreshed_at',
            'newest_available_date',
            'oldest_available_date',
            'column_names',
            'frequency',
            'type',
            'premium'
        ]
        self.qdh.insert_metadata(code_list)
        list_counts = self.quandl_meta.count()
        self.assertEqual(list_counts,len(code_list))
        find_cur = self.quandl_meta.find()
        for code in code_list:
            temp_code_list = code.split('/')
            dataset_code = temp_code_list[1]
            database_code = temp_code_list[0]
            cur = list(self.quandl_meta.find({
                "dataset_code":dataset_code,
                "database_code":database_code}))[0]
            for key in columns_data:
                self.assertTrue(key in cur)

       
 