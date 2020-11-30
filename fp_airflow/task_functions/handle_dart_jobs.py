import asyncio
import os
import pandas as pd
from pymongo import MongoClient
from pendulum import Pendulum
from dataclass_models.models import (
    CompanyReport
)
from task_connector.dart_airflow_connector import DartAirflowConnector
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
    execution_date: Pendulum,
    db_name: str = 'testdb', 
    start_idx: int = 0,
    total_task_count: int = 1,
    only_recents: bool = False,
    **kwargs
):
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    dac = DartAirflowConnector(
        db=db,
        start_idx=start_idx, 
        total_task_count=total_task_count,
    )
    if only_recents:
        data_list = dac.return_current_period_company_list()
    else:
        data_list = dac.return_current_task_companies()
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
    dac = DartAirflowConnector(
        db=db,
        start_idx=start_idx, 
        total_task_count=total_task_count,
    )

    total_request_list = dac.return_insert_needed_link_list()
    chunknum = 10
    total_request_list = [
        total_request_list[i: i + chunknum] 
        for i in range(0, len(total_request_list), chunknum)
    ]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    single_async_request_size = 15
    for start in range(0, len(total_request_list), single_async_request_size):
        end = start + single_async_request_size
        request_list = total_request_list[start:end]
        async_list = [
            handle_dart_report_async_jobs(
                data,
                db,
                ts=ts,
            )
            for data in request_list
        ]
        all_results = asyncio.gather(*async_list, return_exceptions=True)
        results = loop.run_until_complete(all_results)
    for r in results:
        if isinstance(r, Exception):
            print(r) 
    loop.close()