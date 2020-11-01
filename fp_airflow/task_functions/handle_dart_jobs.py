import asyncio
import os
import pandas as pd
from pymongo import MongoClient
from dataclass_models.models import (
    CompanyReport
)
from utils.dart_utils import (
    handle_dart_link_async_jobs,
    handle_dart_report_async_jobs
)


def prepare_company_report_list(
    db_name='testdb', 
    excel_file_path=(
        '/usr/local/airflow/dags'
        '/datacollections/korean_company_list.xlsx'
    ),
    start_idx=0,
    **kwarg
):
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    df = pd.read_excel(excel_file_path)
    df = df.fillna('')
    df = df.sort_values('register_date', ascending=True)
    df = df[0:100]
    db.company_list.delete_many({})
    df['register_date'] = df['register_date'].apply(
        lambda x: x.strftime('%Y%m%d')
    )
    data_list = df.to_dict('records')
    data_list = [CompanyReport(**x).to_json for x in data_list]
    company_code_list = [x['code'] for x in data_list]
    inserted_code_list = list(db.company_list.find(
        {"code": {"$in": company_code_list}},
        {"code": 1}
    ))
    inserted_code_list = [x['code'] for x in inserted_code_list]
    data_list = [x for x in data_list if x['code'] not in inserted_code_list]
    if data_list:
        db.company_list.insert_many(data_list)
    client.close()


def prepare_company_links(
    db_name='testdb', 
    start_idx=0,
    total_task_count=1,
    **kwargs
):
    execution_date = kwargs.get('execution_date')
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    data_list = CompanyReport.return_company_data(
        db.company_list, 
        start_idx, 
        total_task_count
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    jumpnum = 10
    for start in range(0, len(data_list), jumpnum):
        end = start + jumpnum
        request_list = data_list[start:end]
        async_list = [
            handle_dart_link_async_jobs(data, db) for data in request_list
        ]
        all_results = asyncio.gather(*async_list, return_exceptions=True)
        results = loop.run_until_complete(all_results)
    for r in results:
        if isinstance(r, Exception):
            print(r) 
    loop.close()

        
def insert_company_data_list(
    db_name='testdb', 
    start_idx=0,
    total_task_count=1,
    **kwargs
):
    execution_date = kwargs.get('execution_date')
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    data_list = CompanyReport.return_company_data(
        db.company_list, 
        start_idx, 
        total_task_count,
        with_links=True
    )
    total_request_list = []
    for idx, data in enumerate(data_list):
        try:
            link_list = [link_data['link'] for link_data in data['link_list']]
            inserted_link_list = list(db.report_data_list.find(
                {'report_link': {"$in": link_list}},
                {'report_link': 1}
            ))
            inserted_link_list = [x['report_link'] for x in inserted_link_list]
            new_link_data_list = [
                x for x in data['link_list'] 
                if not x['link'] in inserted_link_list
            ]
        except Exception as e: 
            print('Error while get previous report data data ' + str(e))

        jump = 10
        for start in range(0, len(new_link_data_list), jump):
            end = start + jump
            link_list = new_link_data_list[start:end]
            total_request_list.append({
                "link_list": link_list,
                "company": data['company'],
                "code": data['code'],                 
            })
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    jump = 15
    for start in range(0, len(total_request_list), jump):
        end = start + jump
        request_list = total_request_list[start:end]
        async_list = [
            handle_dart_report_async_jobs(
                data['link_list'],
                db,
                ts=ts,
                company=data['company'],
                code=data['code']
            )
            for data in request_list
        ]
        all_results = asyncio.gather(*async_list, return_exceptions=True)
        results = loop.run_until_complete(all_results)
    for r in results:
        if isinstance(r, Exception):
            print(r) 
    loop.close()