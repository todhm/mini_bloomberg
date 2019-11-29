import FinanceDataReader as fdr
from twisted.internet import reactor                
from scrapy.crawler import CrawlerProcess, Crawler,CrawlerRunner
from scrapy.utils.project import get_project_settings
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import pymongo
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from datetime import timedelta,datetime as dt 
import sys 
import pandas as pd 
import requests
import json 
import os 
from math import ceil 
import re 
from utils.class_utils import DataHandlerClass
from utils.mongo_utils import bulk_mongo_inserts,bulk_first_mongo_inserts
from utils.api_utils import return_async_get
from utils.crawl_utils import get_crawl_soup,run_spider
from utils import get_kbd_diff,timestamp
from .NaverFinanceSpider import NaverFinanceSpider
from fp_types import REQUEST_ERROR,BULK_DATA_FAILED,NO_DATA,NO_META_DATA\
                     ,CRAWL_PARSE_ERROR,BULK_DATA_WRITE,DAILY_STOCK_DATA_RESULT,\
                     DAILY_STOCK_DATA

class StockDataHandler(DataHandlerClass):
    def __init__(self,db_name='testdb',*args,**kwargs):
        super().__init__(db_name)
        self.daily_stock_price = self.db.daily_stock_price
        self.crawler_logs = self.db.crawler_logs
        self.korean_company_list = self.db.korean_company_list
        self.db_name = db_name

    def prepare_stock_lists(self):
        df = pd.read_html('./datacollections/korean_firms_list.xls')
        df = df[0]
        col_names = {'회사명':'company_name','종목코드':'code','업종':'company_category',
        '주요제품':'main_products','상장일':'register_date','결산월':'accounting_month','홈페이지':'homepage','지역':'region'}
        df.rename(inplace=True,index=None,columns=col_names)
        self.korean_company_list.delete_many({})
        self.korean_company_list.insert_many(df.to_dict('records'))

    def get_all_stock_list(self):
        return list(self.korean_company_list.find({},{"_id":False}))

    def save_stock_data(self,code_obj):
        stock_code = code_obj.get('code')
        daily_stock_result = list(self.daily_stock_price.find({"code":stock_code},sort=[("date", pymongo.DESCENDING)]).limit(1))
        if not daily_stock_result:
            start = code_obj.get('register_date')
            start = dt.strptime(start,'%Y-%m-%d')
        else:
            last_day = daily_stock_result[0].get('date')
            if not last_day:
                return False 
            start = last_day+timedelta(days=1)
        end = dt.now()
        df = fdr.DataReader(stock_code, start,end)
        df.reset_index(inplace=True)
        df.columns = list(map(lambda x: x.lower(),list(df.columns)))
        df['code'] = stock_code
        self.daily_stock_price.insert_many(df.to_dict('records'))
        return True 


         


    def get_final_page(self,stockcode):
        url = f'https://finance.naver.com/item/sise_day.nhn?code={stockcode}&page=1'
        data = {}
        data['url'] = url
        try:
            soup = get_crawl_soup(url)
        except Exception as e: 
            data['errorMessage'] = str(e)
            self.log.error(REQUEST_ERROR,extra = data)
            return False 
        final_tag = soup.find("td",attrs={"class":"pgRR"})
        if final_tag:
            atag = final_tag.find('a')
        else:
            self.log.error(CRAWL_PARSE_ERROR,extra=data)
            return False 
        if atag:
            final_page_href = atag.attrs['href']
            final_page = re.search("page=(\d+)",final_page_href).group(1)
            final_page = int(final_page)
        else:
            self.log.error(CRAWL_PARSE_ERROR,extra=data)
            return False 
        return final_page

    def insert_url_list(self,stockcode,url_list):
        ts = timestamp()
        try:
            run_spider(
                NaverFinanceSpider,
                db_name=self.db_name, 
                code=stockcode,
                url_list=url_list,
                crawler_logger =self.log, 
                timestamp=ts
            )
        except Exception as e:
            log_data= {}
            log_data['dataName'] = DAILY_STOCK_DATA_RESULT
            log_data['spiderTimestamp'] = ts
            log_data['stockCode'] = stockcode
            log_data['success'] = False
            log_data['errorMessage'] = str(e)
            self.log.error(DAILY_STOCK_DATA_RESULT,extra=log_data)
            

        
        
    def get_daily_url_list(self,stockcode,startDate,final_page):
        if startDate is None:
            url_list = [
                f'https://finance.naver.com/item/sise_day.nhn?code={stockcode}&page={i}'
                for i in range(1,final_page+1)
            ]
            now_time = dt.now()
            today_time = dt(
                year=now_time.year,month=now_time.month,day=now_time.day,
                hour=0,minute=0)
            ts = timestamp(today_time)
            final_crawled_logs =list(self.crawler_logs.find({
                "dataName":DAILY_STOCK_DATA_RESULT,'stockCode':stockcode,
                'spiderTimestamp':{"$gte":ts}}
            ,sort=[("spiderTimestamp", pymongo.DESCENDING)]).limit(1))

            #이전 결과가 실패인경우에는 이전에 실패했던 사이트만 크롤링함.
            if final_crawled_logs:
                last_result = final_crawled_logs[0]
                if not last_result.get("success"):
                    last_ts = last_result['spiderTimestamp']
                    last_failed_sites =list(self.crawler_logs.find({
                        "dataName":DAILY_STOCK_DATA,'stockCode':stockcode,
                        'level':"ERROR",
                        'spiderTimestamp':last_ts,
                        "requestUrl":{"$exists":True}}
                    ,{"requestUrl":1}))
                    url_list = [ url['requestUrl'] for url in last_failed_sites]
        else:
            today = dt.now().date()
            total_dates = get_kbd_diff(startDate,today) 
            total_pages = ceil(total_dates/10)
            url_list = [
                f'https://finance.naver.com/item/sise_day.nhn?code={stockcode}&page={i}'
                for i in range(1,total_pages+1)
            ]
        return url_list
            



        
        
