import asyncio
from datetime import timedelta
from datetime import datetime as dt
import json
import os
from aiohttp import ClientSession
from aiohttp_retry import RetryClient
import pymongo
from pymongo import MongoClient
from utils.api_utils import report_api_call, async_post_functions


def insert_company_data_list(data_list, db_name='testdb'):
    report_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/company_report_data_list'
    eq_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/return_eq_api'
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
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
        if success_list:
            db.report_data_list.insert_many(success_list)
        failed_list = report_data.get("failed_list", [])
        failed_report_link_list = [x['report_link'] for x in failed_list]
        db.failed_report_list.delete_many(
            {"report_link": {"$in": failed_report_link_list}}
        )
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


def insert_company_report_list_with_start_date(data_list, db_name='testdb'):
    report_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/company_report_data_list'
    eq_url = 'https://testcrawler-n7je6n7fnq-an.a.run.app/return_eq_api'
    max_single_request_counts = 10
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    for data in data_list:
        # 마지막 report의 날짜 가져와서 가진데이터는 다시 안불러오는 crawling함수 만들기
        before_data = db.report_data_list.find_one(
            {'code': str(data['code'])},
            {'reg_date': 1},
            sort=[("reg_date", pymongo.DESCENDING)]
        )
        if before_data:
            reg_date = before_data.get("reg_date")
            reg_date += timedelta(days=1)
            start_time = dt.strftime(reg_date, "%Y%m%d")
            data['start_date'] = start_time

    for start in range(0, len(data_list), max_single_request_counts):
        end = start + max_single_request_counts
        request_list = data_list[start:end]
        if start == 0:
            print(request_list)
        # report_list, failure = async_post_functions(
        #     report_url,
        #     request_list
        # )
        # print(failure, 'failure lists')
        # for report_data in report_list:
        #     success_list = report_data.get('success_list', [])
        #     report_link_list = []
        #     for success_data in success_list:
        #         success_data['reg_date'] = dt.strptime(
        #             success_data['reg_date'], '%Y-%m-%d'
        #         )
        #         report_link_list.append(success_data['report_link'])
        #         code = success_data['code']
        #     db.report_data_list.delete_many(
        #         {"report_link": {"$in": report_link_list}}
        #     )
        #     print(len(success_list), code, 'success')
        #     if success_list:
        #         db.report_data_list.insert_many(success_list)
        #     failed_list = report_data.get("failed_list", [])
        #     failed_report_link_list = [x['report_link'] for x in failed_list]
        #     db.failed_report_list.delete_many(
        #         {"report_link": {"$in": failed_report_link_list}}
        #     )
        #     if failed_list:
        #         db.failed_report_list.insert_many(failed_list)

        # eq_list, _ = async_post_functions(eq_url, request_list)
        # for eq_data in eq_list:
        #     eq_link_list = [x['link'] for x in eq_data]
        #     db.eq_offer_list.delete_many(
        #         {"link": {"$in": eq_link_list}}
        #     )
        #     if eq_data:
        #         db.eq_offer_list.insert_many(eq_data)
    client.close()