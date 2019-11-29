import os
import logging
import requests

from .worker import app
from django.conf import settings
from datahandler import StockDataHandler,MetaDataHandler,\
    BokStatHandler,QuandlDataHandler,DartDataHandler
from django.db import connection
from pymongo import MongoClient 
from fp_types import DAILY_STOCK_DATA_RESULT,WORKER_ERROR,WORKER_SUCCESS
from datetime import datetime as dt 
import pymongo 
import django
import celery
import json 


@app.task(bind=True, name='get_stock_daily_data')
def get_stock_daily_data(self,db_name,stockcode,startDate=None,stockName=None):
    sdh = StockDataHandler(db_name)
    result = {}
    result['result'] = "failed"
    final_page = sdh.get_final_page(stockcode)
    if not final_page:
        result['errorMessage'] = '마지막 페이지를 가져오는데 문제가 발생하였습니다.'
        return result 
    
    mongo_client = MongoClient(os.environ.get('MONGO_URI'))
    db = mongo_client[db_name]
    crawler_logs = db.crawler_logs
    daily_stock_price = db.daily_stock_price
    final_crawled_logs =list(crawler_logs.find({
            "dataName":DAILY_STOCK_DATA_RESULT,'stockCode':stockcode}
            ,sort=[("spiderTimestamp", pymongo.DESCENDING)]).limit(1))
    if final_crawled_logs and final_crawled_logs[0].get('level') == "ERROR":
        max_date = None 
    else:
        max_list = list(daily_stock_price.find(
            {"code":stockcode},
            {"date":1},
            sort=[("date", pymongo.DESCENDING)]).limit(1))
        if max_list:
            max_date = max_list[0]['date']
        else:
            max_date = None
    try:
        url_list = sdh.get_daily_url_list(stockcode,max_date,final_page)
    except Exception as e:
        result['errorMessage'] = f'크롤링리스트를 가져오는데 실패하였습니다.{str(e)}'

    try:
        sdh.insert_url_list(stockcode,url_list)
        result['result'] = 'success'

    except Exception as e:
        result['errorMessage'] = f'데이터크롤링에 실패하였습니다.{str(e)}'
    sdh.close()
    return result 




@app.task(bind=True, name='prepare_bok_metadata')
def prepare_bok_metadata(self,db_name='test_fp_data'):
    mdh = MetaDataHandler(db_name)
    result = {}
    result['result'] = "failed"
    
    meta_results = mdh.insert_metadata()
    if not meta_results:
        result['errorMessage'] = "데이터를 가져오는데 실패하였습니다."
        return result 
    
    stat_code_list = mdh.get_all_stats_code()
    total_length = len(stat_code_list)
    jump = 10
    for start in range(0,total_length,jump):
        end = start + jump 
        insert_list = stat_code_list[start:end]
        mdh.insert_item_metadata_list(insert_list)
    result['result'] = 'success'
    return result 





@app.task(bind=True, name='insert_bok_stats')
def insert_bok_stats(self,db_name='test_fp_data'):
    bsh = BokStatHandler(db_name)
    result = {}
    result['result'] = "failed"
    try:
        insert_stats_list = bsh.return_desired_stats()
    except Exception as e:
        result['errorMessage'] = str(e) + "데이터를 가져오는데 실패하였습ㄴ디ㅏ."
        bsh.log.error(WORKER_ERROR,extra=result)        
        return result 
    
    worker_result = bsh.insert_stats_assigned(insert_stats_list)
    if not all(worker_result):
        result['errorMessage'] = "NOT ALL DATA SUCCESS"
        bsh.log.error(WORKER_ERROR,extra = result)
    else:
        result['result'] = "success"
        bsh.log.info(WORKER_SUCCESS)
    return result 


def return_commodity_list():
    with open('datacollections/commodity.json') as f:
        commodity_json = json.load(f)
    commodity_list = [cm['code'] for cm in commodity_json]
    return commodity_list

@app.task(bind=True, name='insert_quandl_stats')
def insert_quandl_stats(self,db_name='test_fp_data'):
    result = {}
    result['result'] = "failed"
    commodity_list = return_commodity_list()
    qdh = QuandlDataHandler(db_name)
    qdh.insert_list(commodity_list,'meta')
    qdh.insert_list(commodity_list,'stats')
    qdh.close()
    result['result'] = 'success'
    return result 


@app.task(bind=True, name='insert_stock_data')
def insert_stock_data(self,db_name='testdb'):
    result = {}
    result['result'] = "failed"
    sdh = StockDataHandler(db_name)
    sdh.prepare_stock_lists()
    stock_list = sdh.get_all_stock_list()
    for codeObj in stock_list:
        is_success = sdh.save_stock_data(codeObj)
        if not is_success:
            return result 
    sdh.close()
    result['result'] = 'success'
    return result 


@app.task(bind=True, name='insert_reportlist_data')
def insert_reportlist_data(self,db_name='testdb'):
    result = {}
    result['result'] = "failed"
    sdh = StockDataHandler(db_name)
    ddh = DartDataHandler(db_name)
    stock_list = sdh.get_all_stock_list()
    for stock in stock_list:
        try:
            ddh.save_company_report_list(stock.get('code'),stock.get('company_name'))
        except Exception as e:
            result['errorMessage'] = str(e)
            return result 
    sdh.close()
    ddh.close()
    result['result'] = 'success'
    return result 

    


@app.task(bind=True, name='insert_all_report_data')
def insert_all_report_data(self,db_name='testdb'):
    result = {}
    result['result'] = "failed"
    sdh = StockDataHandler(db_name)
    ddh = DartDataHandler(db_name)
    stock_list = sdh.get_all_stock_list()
    stock_list = stock_list[1020:]
    print(len(stock_list))
    for idx,stock in enumerate(stock_list):
        print(idx,stock.get('code'))
        try:
            ddh.save_company_all_financial_statements_data(stock.get('code'))
        except Exception as e:
            print(e,stock.get('code'))
            print('a'*100)
            continue

    sdh.close()
    ddh.close()
    result['result'] = 'success'
    return result 

@app.task(bind=True, name='insert_report_data')
def insert_report_data(self,db_name='testdb',**kwargs):
    result = {}
    result['result'] = "failed"
    ddh = DartDataHandler(db_name)
    stock_list = kwargs.get('stockCodeList')
    for idx,code in enumerate(stock_list):
        print(idx,code)
        try:
            ddh.save_company_all_financial_statements_data(code)
        except Exception as e:
            continue

        
    ddh.close()
    result['result'] = 'success'
    return result 