import os 
import pymongo 
from pymongo import MongoClient
from fp_types import DAILY_STOCK_DATA,DAILY_STOCK_DATA_RESULT,\
    SINGLE_DATA_UPDATE,DAILY_STOCK_DATA_RESULT,SINGLE_DATA_INSERT,CRAWL_PARSE_ERROR,\
    INVALID_TRANSACTION
from datetime import datetime as dt 
from scrapy.exceptions import DropItem
import sys 
import os 
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class FpCrawlerPipeline(object):


    def process_item(self, item, spider):
        url = item['url']
        data = spider.log_data
        data['requestUrl'] = url
        data['dataName'] = DAILY_STOCK_DATA
        
        field_list = ['date','end_price','comparison','start_price','highest_price','lowest_price','transactions']
        for key in field_list:
            if key not in item:
                spider.crawler_logger.error(CRAWL_PARSE_ERROR,extra=data)
                spider.success = False
                raise DropItem("Improper data")

        db_name = spider.db_name 
        client = MongoClient(os.environ.get("MONGO_URI"))
        db = client[db_name]
        stock_code_col = db.stock_code_data
        daily_stock_col = db.daily_stock_price

        stock_code_data = list(stock_code_col.find({'code':spider.code}))
        if  not stock_code_data:
            stock_code_col.update_one({'code':spider.code},{"$set":{'code':spider.code}},upsert=True)

        try:
            date = item['date']
            date = dt.strftime(dt.strptime(date,"%Y.%m.%d"),"%Y-%m-%d")
        except Exception as e :
            data['errorMessage'] = 'date parse error'
            spider.crawler_logger.error(CRAWL_PARSE_ERROR,extra=data)
            raise DropItem("Improper data")

        try:
            stockData = {key:val for key,val in item.items() if key in field_list}
            stockData['code'] = spider.code 
            daily_stock_col.update_one({'code':spider.code,'date':date},{"$set":stockData},upsert=True)
            spider.crawler_logger.info(SINGLE_DATA_INSERT,extra=data)
            return True 

        except Exception as e:
            data['errorMessage'] = str(e)
            spider.crawler_logger.error(CRAWL_PARSE_ERROR,extra=data)
            spider.success = False
            raise DropItem("Improper data")
