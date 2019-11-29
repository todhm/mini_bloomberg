import os 
import scrapy
from scrapy.exceptions import DontCloseSpider 
from datahandler.items.items import FpCrawlerItem
import json
import pandas as pd
import re
import os
import itertools
import requests
from urllib.parse import *
from pymongo import MongoClient
import sys 
import os
import sys
import django
from fp_types import DAILY_STOCK_DATA,DAILY_STOCK_DATA_RESULT,\
    SINGLE_DATA_INSERT,CRAWL_PARSE_ERROR,REQUEST_ERROR



class NaverFinanceSpider(scrapy.Spider):
    name = "naver_finance_spider"
    custom_settings = {
        'ITEM_PIPELINES': {
            'datahandler.pipelines.pipelines.FpCrawlerPipeline': 1000
        }
    }

    def __init__(self,db_name,code,url_list,timestamp,crawler_logger,*args, **kwargs):
        super(NaverFinanceSpider, self).__init__(*args, **kwargs)
        self.db_name = db_name
        self.code = code
        self.url_list = url_list 
        self.crawler_logger = crawler_logger 
        self.timestamp = timestamp
        self.success= True 
        self.log_data = {
            "spiderTimestamp":self.timestamp,
            'stockCode':self.code,
        }


    def start_requests(self):
        for url in self.url_list:
            yield scrapy.Request(url,callback=self.parse,dont_filter=True,errback=self.error_back,meta={})

    
    def error_back(self, failure):
        # log all failures
        request = failure.request
        url = request.url
        data = self.log_data
        data['requestUrl'] = url
        data['dataName'] = DAILY_STOCK_DATA
        data['errorMessage'] = repr(failure)
        self.crawler_logger.error(REQUEST_ERROR,extra=data)
        self.success = False 
        raise DontCloseSpider(REQUEST_ERROR)


    def parse(self, response):
        text_list = response.xpath('//table/tr[position()>1]')
        if not text_list:
            data = self.log_data
            data['requestUrl'] = response.url
            data['dataName'] = DAILY_STOCK_DATA
            self.success = False
            self.crawler_logger.error(CRAWL_PARSE_ERROR,extra=data)
            raise DontCloseSpider(CRAWL_PARSE_ERROR)

        text_result = [ text.xpath('./td/span/text()').extract() for text in text_list]
        check_result = any([ len(tr)==7 for tr in text_result])
        if not check_result:
            data = self.log_data
            data['requestUrl'] = response.url
            data['dataName'] = DAILY_STOCK_DATA
            self.success = False 
            self.crawler_logger.error(CRAWL_PARSE_ERROR,extra=data)
            raise DontCloseSpider(CRAWL_PARSE_ERROR)

        for table_data in text_result:
            if not table_data:
                continue
            table_data = [ data.strip().replace(',','') for data in table_data if data ]
            if len(table_data) != 7:
                continue
            return_data = FpCrawlerItem()
            return_data['date']=table_data[0].replace(',','')
            return_data['end_price']=float(table_data[1].replace(',',''))
            return_data['comparison']=float(table_data[2].replace(',',''))
            return_data['start_price']=float(table_data[3].replace(',',''))
            return_data['highest_price']=float(table_data[4].replace(',',''))
            return_data['lowest_price']=float(table_data[5].replace(',',''))
            return_data['transactions']=float(table_data[6].replace(',',''))
            return_data['url'] = response.url
            yield return_data


    
    
    def close(self, reason):
        data = {
            "spiderTimestamp":self.timestamp,
            'dataName':DAILY_STOCK_DATA_RESULT,
            'stockCode':self.code,
            'success':self.success
        }

        if self.success:
            self.crawler_logger.info(DAILY_STOCK_DATA_RESULT,extra=data)
        else:
            self.crawler_logger.error(DAILY_STOCK_DATA_RESULT,extra=data)




       