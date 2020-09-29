from math import ceil
from dataclasses import dataclass
from datetime import datetime as dt
from dataclasses import asdict, fields



@dataclass 
class CompanyReport:

    code: str = ""
    company: str = ""
    company_category: str = ""
    main_products: str = ""
    start_date: str = ''
    register_date: dt = dt.now()
        
    def __init__(self, *args, **kwargs):
        names = set([f.name for f in fields(self)])
        added_names = set()
        for k, v in kwargs.items():
            if k in names and k != 'register_date':
                added_names.add(k)
                setattr(self, k, v)
            elif k.lower() in names and k != 'register_date':
                added_names.add(k.lower())
                setattr(self, k, v)
        for f in fields(self):
            if f.name not in added_names and f.name != 'register_date':
                setattr(self, f.name, f.default)
        if kwargs.get('register_date'):
            register_date = dt.strptime(kwargs['register_date'], '%Y%m%d')
            setattr(self, 'register_date', register_date)

    @property 
    def to_json(self):
        return asdict(self)
        

    @classmethod
    def return_company_data(cls, db_col, start_idx, total_task_counts):
        company_counts = db_col.count_documents({})
        if start_idx >= 0:
            single_page_company_counts = ceil(company_counts/total_task_counts)
        else:
            single_page_company_counts = company_counts
        page = start_idx + 1
        offset = (page - 1) * single_page_company_counts
        if offset > 0:
            search_table_results = (
                db_col
                .find({}, {"_id": False})
                .skip(offset)
                .limit(single_page_company_counts)
            )
        else:
            search_table_results = (
                db_col
                .find({}, {"_id": False})
                .limit(single_page_company_counts)
            )
        data_list = list(search_table_results)
        for x in data_list:
            x['register_date'] = dt.strftime(x['register_date'], '%Y%m%d')
        return data_list
            







            

