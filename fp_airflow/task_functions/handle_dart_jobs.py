from datetime import datetime as dt
import os
import pandas as pd
import pymongo
from pymongo import MongoClient
from dataclass_models.models import CompanyReport
import requests
import json


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
    df = df[:60]
    df['register_date'] = df['register_date'].apply(lambda x: x.strftime('%Y%m%d'))
    data_list = df.to_dict('records')
    data_list = [CompanyReport(**x).to_json for x in data_list]
    db.company_list.insert_many(data_list)
    client.close()


def insert_company_data_list(
    db_name='testdb', 
    start_idx=0,
    total_task_counts=1,
    **kwargs
):
    execution_date = kwargs.get('execution_date')
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    # report_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/company_report_data_list'
    # eq_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/return_eq_api'
    report_url = 'http://fpgunicorn:8000/company_report_data_list'
    eq_url = 'http://fpgunicorn:8000/return_eq_api'
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    data_list = CompanyReport.return_company_data(
        db.company_list, 
        start_idx, 
        total_task_counts
    )
    for idx, data in enumerate(data_list):
        print(idx, data['company'])
        try:
            response = requests.post(
                eq_url,
                json=data,
            )
            eq_data = json.loads(response.text)
            if eq_data:
                eq_link_list = [x['link'] for x in eq_data]
                db.eq_offer_list.delete_many(
                    {"link": {"$in": eq_link_list}}
                )
                for eq in eq_data:
                    eq['timestamp'] = ts
                    eq['reg_date'] = dt.strptime(
                        eq['reg_date'], '%Y-%m-%d'
                    )
                db.eq_offer_list.insert_many(eq_data)
        except Exception as e: 
            print('Error while crawling eq data ' + str(e))

        try:
            response = requests.post(
                report_url,
                json=data,
            )
            if response.status_code != 200:
                print(response.text, 'Error')
                continue
            report_data_json = json.loads(response.text)
            success_list = report_data_json['success_list']
            failed_list = report_data_json['failed_list']
            print(data, 'Success report links', len(success_list))
            print(data, 'Failed report links', len(failed_list))
            if success_list:
                link_list = [x['report_link'] for x in success_list]
                for report_data in success_list:
                    report_data['timestamp'] = ts
                    report_data['reg_date'] = dt.strptime(
                        report_data['reg_date'], '%Y-%m-%d'
                    )
                db.report_data_list.delete_many(
                    {'report_link': {"$in": link_list}}
                )
                db.report_data_list.insert_many(success_list)
            if failed_list:
                for failed_data in failed_list:
                    failed_data['timestamp'] = ts
                db.failed_report_list.insert_many(failed_list)
        except Exception as e: 
            print('Error while calling report data data ' + str(e))


def insert_continuous_company_report_list(
    db_name='testdb', 
    start_idx=0,
    total_task_counts=1,
    **kwargs
):
    execution_date = kwargs.get('execution_date')
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    # report_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/company_report_data_list'
    # eq_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/return_eq_api'
    report_url = 'http://fpgunicorn:8000/company_report_data_list'
    eq_url = 'http://fpgunicorn:8000/return_eq_api'
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    data_list = CompanyReport.return_company_data(
        db.company_list, 
        start_idx, 
        total_task_counts
    )
    for idx, data in enumerate(data_list):
        print(idx, data['company'])
        try:
            response = requests.post(
                eq_url,
                json=data,
            )
            eq_data = json.loads(response.text)
            if eq_data:
                eq_link_list = [x['link'] for x in eq_data]
                db.eq_offer_list.delete_many(
                    {"link": {"$in": eq_link_list}}
                )
                for eq in eq_data:
                    eq['timestamp'] = ts
                    eq['reg_date'] = dt.strptime(
                        eq['reg_date'], '%Y-%m-%d'
                    )
                db.eq_offer_list.insert_many(eq_data)
        except Exception as e: 
            print('Error while crawling eq data ' + str(e))
        inserted_report_list = list(
            db
            .report_data_list
            .find({'code': str(data['code'])}, {'reg_date': True})
            .sort('reg_date', pymongo.DESCENDING)
            .limit(1)
        )
        try:
            print(inserted_report_list)
            if inserted_report_list:
                data['start_date'] = dt.strftime(
                    inserted_report_list[0]['reg_date'],
                    '%Y%m%d'
                )
        except Exception as e: 
            print("Error while parsing latest report data" + str(e))
            continue

        try:
            response = requests.post(
                report_url,
                json=data,
            )
            if response.status_code != 200:
                print(response.text, 'Error')
                continue
            report_data_json = json.loads(response.text)
            success_list = report_data_json['success_list']
            failed_list = report_data_json['failed_list']
            print(data, 'Success report links', len(success_list))
            print(data, 'Failed report links', len(failed_list))
            if success_list:
                link_list = [x['report_link'] for x in success_list]
                for report_data in success_list:
                    report_data['timestamp'] = ts
                    report_data['reg_date'] = dt.strptime(
                        report_data['reg_date'], '%Y-%m-%d'
                    )
                db.report_data_list.delete_many(
                    {'report_link': {"$in": link_list}}
                )
                db.report_data_list.insert_many(success_list)
            if failed_list:
                for failed_data in failed_list:
                    report_data['timestamp'] = ts
                db.failed_report_list.insert_many(failed_list)
        except Exception as e: 
            print('Error while calling report data data ' + str(e))



            