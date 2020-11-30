import json
from datetime import datetime as dt
from aiohttp import ClientSession
from dataclass_models.models import (
    CompanyReport
)
from pymongo.database import Database
from typing import Dict, List
import os


async def handle_dart_report_async_jobs(
    link_list: List[Dict], 
    db: Database, 
    ts: int,
):
    link_url = os.environ.get("FPCRAWLER_URL")
    report_url = f'{link_url}/report'
    post_data = {'linkList': link_list}
    async with ClientSession() as session:
        async with session.post(
            report_url, 
            json=post_data, 
        ) as response:
            try:
                text = await response.text()
                report_data_json = json.loads(text)
                if response.status != 200:
                    error_message = report_data_json.get("errorMessage", '')
                    raise ValueError(
                        "Not a valid request " 
                        + error_message 
                    )
            except Exception as e: 
                raise ValueError(
                    str(e) 
                )
    
    success_list = report_data_json['success_list']
    failed_list = report_data_json['failed_list']
    print(
        'Success report links', 
        len(success_list)
    )
    print(
        'Failed report links', 
        len(failed_list)
    )
    if success_list:
        try:
            for report_data in success_list:
                report_data['timestamp'] = ts
                report_data['reg_date'] = dt.strptime(
                    report_data['reg_date'], '%Y-%m-%d'
                )
            db.report_data_list.insert_many(success_list)
        except Exception as e:
            print(
                'Error while inserting report list ' 
                + str(e)
            )
            if failed_list:
                for failed_data in failed_list:
                    print(
                        failed_data.get("report_link"), 
                        failed_data.get('table_link'),
                        failed_data.get("report_types")
                    )
                    failed_data['timestamp'] = ts
                db.failed_report_list.insert_many(failed_list)


async def handle_dart_link_async_jobs(data: Dict, db: Database):
    link_url = os.environ.get("FPCRAWLER_URL")
    report_url = f'{link_url}/links'
    post_data = {
        'code': data['code'], 
        'company': data['company'],
        'start_date': data.get('start_date', None)
    }
    async with ClientSession() as session:
        async with session.post(
            report_url, 
            json=post_data, 
        ) as response:
            try:
                text = await response.text()
                json_data = json.loads(text)
                if response.status != 200:
                    error_message = json_data.get("errorMessage", '')
                    raise ValueError(
                        "Not a valid request " 
                        + error_message 
                        + f"{data['code']} {data['company']}"
                    )
            except Exception as e: 
                raise ValueError(
                    str(e) 
                    + f"{data['code']} {data['company']}"
                )
            data['link_list'] = json_data
            crd = CompanyReport(**data)
            crd.update_link(db.company_list)
            link_length = len(json_data)
            print(
                f"{link_length} links " 
                f"for company {data['company']} {data['code']}"
            )
