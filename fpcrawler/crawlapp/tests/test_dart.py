
from django.test import SimpleTestCase,TestCase,override_settings
from rest_framework.test import APIClient, APITestCase
from django.urls import reverse 
from utils.task_utils import launch_task
from django.test import tag
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
from datahandler.dartdatahandler import DartDataHandler
from utils.api_utils import return_async_get_soup,return_sync_get_soup,ReturnSoupError


save_report_url=reverse('crawlapp:save_report_data')



def save_report_list_data(company_report_col,**params):
    defaults = {
    "code" : '3670', 
    "link" : "http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20060512002342", 
    "reg_date" : '2018-11-11',
    "corp_name" : "포스코케미칼", 
    "market_type" : "유가증권시장", 
    "title" : "[기재정정]사업보고서 (2005.12)", 
    "period_type" : "사업보고서",
     "reporter" : "포스코케미칼"
    }
    defaults.update(params)
    company_report_col.insert_one(defaults)
    return defaults


@tag('dart')
class DartTest(TestCase):
    def setUp(self):
        super().setUp()
        self.mongo_client = MongoClient(os.environ.get('MONGO_URI'))
        self.db = self.mongo_client.testdb
        self.company_report_list = self.db.company_report_list
        self.company_data_list = self.db.company_data_list
        self.ddh = DartDataHandler('testdb')

        

    def test_django_requests(self):
        save_report_list_data(self.company_report_list)
        data = list(self.company_report_list.find({},{'code':1}))
        data = [ x['code'] for x in data]
        response = self.client.post(save_report_url,data=dict(
            stockCodeList=data
        ))
        task_id = response.data['taskId']
        url_data = {}
        url_data['nk'] = task_id
        get_task_url = reverse('crawlapp:task',kwargs=url_data)
        while True: 
            res = self.client.get(get_task_url)
            if res.data.get('complete') ==True or res.status_code != 200: 
                break 
        self.assertTrue(len(list(self.company_data_list.find()))>=1)

    @tag('dart2')
    def test_dart_link_parse(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20121114000100'
        soup = return_sync_get_soup(link)
        data = self.ddh.parse_financial_section_link(soup)
        

    def tearDown(self):
        super().tearDown()
        self.mongo_client.drop_database('testdb')
        self.mongo_client.close()
        