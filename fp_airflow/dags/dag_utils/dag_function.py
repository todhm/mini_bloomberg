import os 
from .dag_helpers import call_async_func
from pymongo import MongoClient
from airflow import AirflowException
import json 
import os 
import requests
from datetime import datetime as dt


def convert_to_json(x):
    x['reg_date'] = dt.strftime(x['reg_date'],'%Y-%m-%d')
    return x 

def crawl_daily_stock(**kwargs):
    base_crawler_url = os.environ.get("base_crawler_url")
    target_stock_code_list = ['018250']
    for stock_code in target_stock_code_list:
        print(stock_code)


   

def handle_dart_jobs(**kwargs):
    total_number = kwargs.get('total_number')
    index = kwargs.get('index')
    mongo_uri = os.environ.get("MONGO_URI")
    lambda_url = os.environ.get("LAMBDA_URL")
    mongo = MongoClient(mongo_uri)
    server_mongo_url = os.environ.get('SERVER_MONGO')
    server_mongo = MongoClient(server_mongo_url).fp_data
    db = mongo.fp_data
    col = db.korean_company_list
    company_data = server_mongo.company_data_list
    company_report_list = db.company_report_list
    find_table = col.find({},{"code":1,'company_name':1})
    total_count = find_table.count()
    per_page = int(total_count/total_number)
    page = index + 1
    offset = (page - 1) * per_page
    if offset >0:
        search_table_results = find_table.skip(offset).limit(per_page)
    else:
        search_table_results = find_table.limit(per_page)
    search_table_results = list(search_table_results)

    for search_table in search_table_results:
        code = search_table.get('code')
        title_list = list(company_data.find({"company_code":int(code)},{'link':1}))
        link_list = [ x['link'] for x in title_list if x.get('link')]
        report_list = list(company_report_list.find(
            {"code":int(code),"link":{"$nin":link_list}},
            {"link":1,'reg_date':1,'report_name':1,'period_type':1,'company_code':1,"_id":False}
        ))
        report_list = list(map(convert_to_json,report_list))
        jump_num = 10 
        for start in range(0,len(report_list),jump_num):
            end = start + jump_num 
            post_report_list = report_list[start:end]
            post_data = {}
            post_data['report_list'] = post_report_list
            try: 
                response = requests.post(lambda_url+'/save_report_data',json=post_data)
                result = json.loads(response.text)
                result_list = result.get('data')
                if result_list:
                    company_data.insert_many(result_list)
            except Exception as e:
                print(report_list)
                print(e)
                print('a'*100)
                
            if response.status_code > 300: 
                print('b'*100)
                print(response.text)
                print(report_list)
                
        

    
    
    