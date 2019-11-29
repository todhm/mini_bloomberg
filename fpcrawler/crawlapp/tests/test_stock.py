
from django.test import SimpleTestCase,TestCase,override_settings
from rest_framework.test import APIClient, APITestCase
from django.urls import reverse 
from utils.task_utils import launch_task
from worker.tasks import get_stock_daily_data
from celery.result import AsyncResult 
from pymongo import MongoClient
import os 
from datahandler.stockdatahandler import StockDataHandler
from fp_types import DAILY_STOCK_DATA,DAILY_STOCK_DATA_RESULT,\
    BULK_DATA_FAILED,NO_DATA,DAILY_STOCK_DATA_RESULT,BULK_DATA_WRITE
from utils import timestamp
from datetime import datetime as dt
from celery.contrib.testing.worker import start_worker
from worker.worker import app 
from unittest.mock import patch



def create_stock_code(code="018250",name='애경산업'):
    stockCode = StockCodeData(code=code,name=name)
    stockCode.save()   
    return stockCode 


def get_all_daily_stock(col,code="018250"):
    return list(col.find({"code":code}))

class StockTest(SimpleTestCase):
    allow_database_queries = True 
    

    def setUp(self):
        super().setUp()
        self.client = APIClient()
        self.mongo_client = MongoClient(os.environ.get('MONGO_URI'))
        self.db = self.mongo_client.test_fp_data
        self.stockcode = '018250'
        self.stockName = '애경산업'
        self.db_name='test_fp_data'
        self.daily_stock_price = self.db.daily_stock_price
        self.crawler_logs = self.db.crawler_logs
        self.sdh = StockDataHandler('test_fp_data')
        self.ts = timestamp()
        self.test_url_list = ['https://finance.naver.com/item/sise_day.nhn?code=005930&page=1']



    def tearDown(self):
        super().tearDown()
        self.mongo_client.drop_database('test_fp_data')
        self.mongo_client.close()


    @patch('worker.tasks.StockDataHandler.get_final_page')
    def test_get_daily_stock_result_when_finalpage_failed(self,final_page):
        final_page.return_value=False
        result = get_stock_daily_data('test_fp_data',self.stockcode,None,self.stockName) 
        final_page.assert_called_with(self.stockcode)
        self.assertEqual(result['result'],'failed')


    @patch('worker.tasks.StockDataHandler.get_final_page')
    @patch('worker.tasks.StockDataHandler.get_daily_url_list')
    def test_get_daily_stock_when_final_page(self,get_url_list,final_page):
        final_page.return_value=1
        get_url_list.return_value=self.test_url_list
        result = get_stock_daily_data('test_fp_data',self.stockcode,None,self.stockName) 
        final_page.assert_called_with(self.stockcode)
        self.assertEqual(result['result'],'success')
        daily_stock_price_list = get_all_daily_stock(self.daily_stock_price)
        self.assertEqual(len(daily_stock_price_list),10)



    @patch('worker.tasks.StockDataHandler.get_final_page')
    def test_get_daily_stock_called_properly(self,final_page):
        get_stock_daily_data('test_fp_data',self.stockcode,None,self.stockName) 
        final_page.assert_called_with(self.stockcode)



    @patch('worker.tasks.StockDataHandler.get_final_page')
    @patch('worker.tasks.StockDataHandler.get_daily_url_list')
    def test_daily_stock_api(self,get_url_list,final_page):
        final_page.return_value=1
        get_url_list.return_value=self.test_url_list
        response = self.client.post(reverse('crawlapp:launch_daily_stock'),data=dict(
            stockCode=self.stockcode,
            startDate='2019-01-01T00:00',
        ))
        task_id = response.data['taskId']
        url_data = {}
        url_data['nk'] = task_id
        get_task_url = reverse('crawlapp:task',kwargs=url_data)
        while True: 
            res = self.client.get(get_task_url)
            if res.data.get('complete') ==True or res.status_code != 200: 
                break 
        url_log = list(self.crawler_logs.find({'dataName':DAILY_STOCK_DATA}))
        daily_stock_price_list = get_all_daily_stock(self.daily_stock_price)
        self.assertTrue(len(daily_stock_price_list)>10)

    def test_run_proper_spider(self):
        self.sdh.insert_url_list(self.stockcode,self.test_url_list)
        self.sdh.close()
        daily_stock_price_list = get_all_daily_stock(self.daily_stock_price)
        self.assertEqual(len(daily_stock_price_list),10)
        final_result = list(self.crawler_logs.find({'dataName':DAILY_STOCK_DATA_RESULT}))
        self.assertEqual(len(final_result),1)
        self.assertEqual(final_result[0]['level'],"INFO")

    def test_run_spider_multiple_page(self):
        self.sdh.insert_url_list(
            self.stockcode,
            [
                'https://finance.naver.com/item/sise_day.nhn?code=005930&page=1',
                'https://finance.naver.com/item/sise_day.nhn?code=005930&page=2'
            ])
        self.sdh.close()
        daily_stock_price_list = get_all_daily_stock(self.daily_stock_price)
        self.assertEqual(len(daily_stock_price_list),20)
        final_result = list(self.crawler_logs.find({'dataName':DAILY_STOCK_DATA_RESULT}))
        self.assertEqual(len(final_result),1)
        self.assertEqual(final_result[0]['level'],"INFO")
        
    def test_error_and_none_error(self):
        url_list = ['https://www.amazon.com/s?k=python3&rh=n%3A154606011&ref=nb_sb_noss','https://finance.naver.com/item/sise_day.nhn?code=005930&page=1']
        self.sdh.insert_url_list(self.stockcode,url_list)
        self.sdh.close()
        daily_stock_price_list = get_all_daily_stock(self.daily_stock_price)
        self.assertEqual(len(daily_stock_price_list),10)

        final_result = list(self.crawler_logs.find({'dataName':DAILY_STOCK_DATA_RESULT}))
        self.assertEqual(len(final_result),1)
        self.assertEqual(final_result[0]['level'],"ERROR")

    def test_error_spider(self):
        url_list = ['https://www.amazon.com/s?k=python3&rh=n%3A154606011&ref=nb_sb_noss']
        self.sdh.insert_url_list(self.stockcode,url_list)   
        self.sdh.close()
        url_log = list(self.crawler_logs.find({'dataName':DAILY_STOCK_DATA}))
        self.assertEqual(url_log[0]['level'],"ERROR")
        log_list = list(self.crawler_logs.find({'dataName':DAILY_STOCK_DATA_RESULT}))
        failed_log = log_list[0]
        self.assertEqual(failed_log['dataName'],DAILY_STOCK_DATA_RESULT)
        self.assertEqual(failed_log['success'],False)
