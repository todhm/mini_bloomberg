from datetime import datetime as dt
import os
import pandas as pd
from pymongo import MongoClient
from dataclass_models.models import CompanyReport
from utils.request_utils import report_api_call, async_post_functions


def prepare_company_report_list(
    db_name='testdb', 
    excel_file_path='/usr/local/airflow/dags/datacollections/korean_company_list.xlsx',
    start_idx=0,
    **kwarg
):
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    db.company_list.delete_many({})
    df = pd.read_excel(excel_file_path)
    df = df.fillna('')
    df = df.sort_values('register_date', ascending=True)
    df['register_date'] = df['register_date'].apply(lambda x: x.strftime('%Y%m%d'))
    data_list = df.to_dict('records')
    data_list = [CompanyReport(**x).to_json for x in data_list]
    db.company_list.insert_many(data_list)
    client.close()


def insert_company_data_list(
    db_name='testdb', 
    start_idx=0,
    total_task_counts=1,
    **kwarg
):

    report_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/company_report_data_list'
    eq_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/return_eq_api'
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    data_list = CompanyReport.return_company_data(
        db.company_list, 
        start_idx, 
        total_task_counts
    )
    report_list, failure = report_api_call(
        report_url,
        data_list
    )
    print(failure, 'failure lists')
    for report_data in report_list:
        success_list = report_data.get('success_list', [])
        report_link_list = []
        for success_data in success_list:
            success_data['reg_date'] = dt.strptime(
                success_data['reg_date'], '%Y-%m-%d'
            )
            report_link_list.append(success_data['report_link'])
        db.report_data_list.delete_many(
            {"report_link": {"$in": report_link_list}}
        )
        print(len(success_list), 'success')
        if success_list:
            db.report_data_list.insert_many(success_list)
        failed_list = report_data.get("failed_list", [])
        failed_report_link_list = [x['report_link'] for x in failed_list]
        db.failed_report_list.delete_many(
            {"report_link": {"$in": failed_report_link_list}}
        )
        print(len(failed_list), 'success')
        if failed_list:
            db.failed_report_list.insert_many(failed_list)
    eq_list, _ = async_post_functions(eq_url, data_list)
    for eq_data in eq_list:
        eq_link_list = [x['link'] for x in eq_data]
        db.eq_offer_list.delete_many(
            {"link": {"$in": eq_link_list}}
        )
        for eq in eq_data:
            eq['reg_date'] = dt.strptime(
                eq['reg_date'], '%Y-%m-%d'
            )
        if eq_data:
            db.eq_offer_list.insert_many(eq_data)
    client.close()