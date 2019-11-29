
from django.test import SimpleTestCase,TestCase,override_settings
from rest_framework.test import APIClient, APITestCase
from django.urls import reverse 
from utils.task_utils import launch_task
from worker.tasks import prepare_bok_metadata,insert_bok_stats,insert_quandl_stats
from celery.result import AsyncResult 
from pymongo import MongoClient
import os 
from datahandler.metadatahandler import MetaDataHandler
from fp_types import WORKER_SUCCESS
from utils import timestamp
from datetime import datetime as dt
from celery.contrib.testing.worker import start_worker
from worker.worker import app 
from unittest.mock import patch



BOK_WORKER_URL=reverse('crawlapp:crawl_quandl_data')
def get_daily_metadata(col):
    return list(col.find())
commodity_list =  ['WORLDBANK/WLD_SOYBEAN_OIL','CHRIS/CME_BO1']
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
class QuandlTest(SimpleTestCase):
    allow_database_queries = True 
    

    def setUp(self):
        super().setUp()
        self.client = APIClient()
        self.mongo_client = MongoClient(os.environ.get('MONGO_URI'))
        self.db = self.mongo_client.test_fp_data
        self.bok_item_code = self.db.bok_item_code
        self.bok_stat_code = self.db.bok_stat_code
        self.bok_stats = self.db.bok_stats

        self.crawler_logs = self.db.crawler_logs
        self.mdh = MetaDataHandler('test_fp_data')

    def check_inserted_quandl_data(self,code_list):
        for code in code_list:
            temp_code_list = code.split('/')
            dataset_code = temp_code_list[1]
            database_code = temp_code_list[0]
            cur = list(self.quandl_meta.find({
                "dataset_code":dataset_code,
                "database_code":database_code}))[0]
            columns = cur['column_names']
            data_id = cur['id']
            data_list = list(self.quandl_stats.find({"id":data_id}))
            assert len(data_list) >0
            for stat in data_list:
                for col in columns:
                    col = col.replace('.','')
                    assert col in stat 


    @patch('worker.tasks.return_commodity_list')
    def test_quandl_multiple_data(self,commodity_list_func):

        commodity_list_func.return_value = commodity_list
        result = insert_quandl_stats()
        self.assertEqual(result['result'],'success')
        for code in commodity_list:
            temp_code_list = code.split('/')
            dataset_code = temp_code_list[1]
            database_code = temp_code_list[0]
            cur = list(self.quandl_meta.find({
                "dataset_code":dataset_code,
                "database_code":database_code}))[0]
            for key in columns_data:
                assert key in cur 
        self.check_inserted_quandl_data(commodity_list)
        
    @patch('worker.tasks.return_commodity_list')
    def test_quandl_multiple_times(self,commodity_list_func):

        commodity_list_func.return_value = commodity_list
        result = insert_quandl_stats()
        result = insert_quandl_stats()
        self.assertEqual(result['result'],'success')
        for code in commodity_list:
            temp_code_list = code.split('/')
            dataset_code = temp_code_list[1]
            database_code = temp_code_list[0]
            cur = list(self.quandl_meta.find({
                "dataset_code":dataset_code,
                "database_code":database_code}))[0]
            for key in columns_data:
                assert key in cur 
        self.check_inserted_quandl_data(commodity_list)


    def tearDown(self):
        super().tearDown()
        self.mongo_client.drop_database('test_fp_data')
        self.mongo_client.close()

