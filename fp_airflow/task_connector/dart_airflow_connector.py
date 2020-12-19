from math import ceil
from typing import Optional
from datetime import datetime as dt, timedelta
from pymongo.database import Database
from utils.common import current_time

# from dataclass_models.models import TaskArgumentsList, 


class DartAirflowConnector(object):

    def __init__(
        self, 
        db: Database,
        start_idx: int, 
        total_task_count: Optional[int], 
    ):
        self.db = db
        self.start_idx = start_idx
        self.total_task_count = total_task_count

    def return_current_task_companies(
        self,
        with_links: bool = False,
        only_recents: bool = False
    ):
        company_counts = self.db.company_list.count_documents({})
        if self.start_idx >= 0:
            single_page_company_counts = ceil(
                company_counts/self.total_task_count
            )
        else:
            single_page_company_counts = company_counts
        page = self.start_idx + 1
        offset = (page - 1) * single_page_company_counts
        if with_links:
            fetch_dict = {"_id": False}
        else:
            fetch_dict = {"_id": False, 'link_list': False}
        if offset > 0:            
            search_table_results = (
                self.db.company_list
                .find({}, fetch_dict)
                .skip(offset)
                .limit(single_page_company_counts)
            )
        else:
            search_table_results = (
                self.db.company_list
                .find({}, fetch_dict)
                .limit(single_page_company_counts)
            )
        data_list = list(search_table_results)
        for x in data_list:
            if type(x['register_date']) is not str:
                x['register_date'] = dt.strftime(x['register_date'], '%Y%m%d')
        return data_list

    def return_insert_needed_link_list(
        self,
        only_recents: bool = False
    ):
        data_list = self.return_current_task_companies(
            with_links=True,
        )
        total_request_list = []
        for idx, data in enumerate(data_list):
            try:
                link_list = [
                    link_data['link'] for link_data in data['link_list']
                ]
                inserted_link_list = list(self.db.report_data_list.find(
                    {'report_link': {"$in": link_list}},
                    {'report_link': 1}
                ))
                inserted_link_list = [
                    x['report_link'] for x in inserted_link_list
                ]
                new_link_data_list = [
                    x for x in data['link_list'] 
                    if not x['link'] in inserted_link_list
                ]
                total_request_list.extend(new_link_data_list)
            except Exception as e: 
                print('Error while get previous report data ' + str(e))
        return total_request_list
    
    def return_current_period_company_list(
        self
    ):
        company_counts = self.db.company_list.count_documents({})
        if self.start_idx >= 0:
            single_page_company_counts = ceil(
                company_counts/self.total_task_count
            )
        else:
            single_page_company_counts = company_counts
        page = self.start_idx + 1
        offset = (page - 1) * single_page_company_counts
        fetch_dict = {"_id": False}
        if offset > 0:            
            search_table_results = (
                self.db.company_list
                .find({}, fetch_dict)
                .skip(offset)
                .limit(single_page_company_counts)
            )
        else:
            search_table_results = (
                self.db.company_list
                .find({}, fetch_dict)
                .limit(single_page_company_counts)
            )
        data_list = list(search_table_results)
        for x in data_list:
            recent_report_data = self.db.report_data_list.find_one(
                {'code': str(x['code'])},
                {'reg_date': 1},
                sort=[('reg_date', -1)]
            )
            nowtime = current_time()
            two_years_days = ceil(365.25 * 2)
            start_period_time = nowtime - timedelta(days=two_years_days)
            if recent_report_data:
                recent_max_date = max(
                    start_period_time, recent_report_data['reg_date']
                )
            else:
                recent_max_date = start_period_time
            x['start_date'] = dt.strftime(recent_max_date, '%Y%m%d')
            if type(x['register_date']) is not str:
                x['register_date'] = dt.strftime(x['register_date'], '%Y%m%d')
        return data_list
