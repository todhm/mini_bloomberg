
from django.test import SimpleTestCase,TestCase,override_settings
from rest_framework.test import APIClient, APITestCase
from django.urls import reverse 
from utils.task_utils import launch_task
from worker.tasks import prepare_bok_metadata,insert_bok_stats
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



BOK_WORKER_URL=reverse('crawlapp:crawl_bok_metadata')
def get_daily_metadata(col):
    return list(col.find())


class BokTest(SimpleTestCase):
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


    @patch('worker.tasks.MetaDataHandler.get_all_stats_code')
    def test_bok_worker(self,get_all_stats_code):
        get_all_stats_code.return_value = ['088Y011','088Y012','088Y004']
        result = prepare_bok_metadata()
        self.assertEqual(result['result'],'success')
        for key in get_all_stats_code.return_value:
            search_result = list(self.bok_item_code.find({"STAT_CODE":key}))
            self.assertTrue(len(search_result)>0)


    @patch('worker.tasks.BokStatHandler.return_desired_stats')
    def test_bok_stats_work(self,desired_stats):
        test_item_code = {
            "ITEM_CODE" : "010230000", "STAT_CODE" : "060Y001",
             "CYCLE" : "DD", "DATA_CNT" : 1580, "END_TIME" : "20190211",
              "GRP_CODE" : "Group1", "GRP_NAME" : "항목선택", 
              "ITEM_NAME" : "국고채(30년)", "P_ITEM_CODE" : "", 
              "START_TIME" : "19950103", 
              "STAT_NAME" : "4.1.1 시장금리(일별)" 
        }
        self.bok_item_code.insert_one(test_item_code)
        desired_stats.return_value = ['060Y001']
        result = insert_bok_stats()
        self.assertEqual(result['result'],'success')
        for key in desired_stats.return_value:
            search_result = list(self.bok_item_code.find({"STAT_CODE":key}))
            self.assertTrue(len(search_result)>0)
        log_list = list(self.crawler_logs.find({"message":WORKER_SUCCESS}))
        self.assertEqual(len(log_list),1)




    def tearDown(self):
        super().tearDown()
        self.mongo_client.drop_database('test_fp_data')
        self.mongo_client.close()

