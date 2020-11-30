from math import ceil
from datetime import datetime as dt
from dataclasses import (
    dataclass, asdict, fields, field
)
from pymongo.database import Database
from pymongo.collection import Collection
from typing import List, Dict


@dataclass 
class CompanyReport:
    code: str = ""
    company: str = ""
    company_category: str = ""
    main_products: str = ""
    start_date: str = ''
    link_list: List[Dict] = field(default_factory=list)
    register_date: dt = dt.now()
        
    def __init__(self, *args, **kwargs):
        names = set([f.name for f in fields(self)])
        added_names = set()
        for k, v in kwargs.items():
            if (k in names or k.lower() in names):
                added_names.add(k)
                setattr(self, k, v)
        for f in fields(self):
            if (
                f.name == 'link_list'
                and 
                f.name not in added_names
            ):
                setattr(self, f.name, [])
        
    @property 
    def to_json(self):
        register_date = dt.strptime(self.register_date, '%Y%m%d')
        data = asdict(self)
        data['register_date'] = register_date
        return data

    def update_link(
        self,
        db_col: Collection,
    ):
        data_json = asdict(self)
        db_col.update_one(
            {'code': self.code}, 
            {"$set": data_json}, 
            upsert=True
        )
                

@dataclass 
class TaskArgumentsList:
    timestamp: int = None
    dataList: List = field(default_factory=list)
    taskName: str = ""

    def save(self, db: Database):
        save_data = asdict(self)
        db.airflow_task_list.insert_one(save_data)

    @classmethod
    def fetch_current_args(
        cls,
        db: Database, 
        taskName: str, 
        timestamp: int,
        currentIdx: int = 0, 
        totalTaskLength: int = 1
    ) -> List:
        task_argument_doc = db.airflow_task_list.find_one({
            'taskName': taskName, 
            "timestamp": timestamp
        })
        data_list = task_argument_doc['dataList']
        each_length = ceil(len(data_list) / totalTaskLength)
        start = each_length * currentIdx
        end = each_length * (currentIdx + 1)
        return data_list[start:end]

    @classmethod
    def fetch_total_arguments(
        cls,
        db: Database, 
        taskName: str, 
        timestamp: int,
    ) -> List:
        task_argument_doc = db.airflow_task_list.find_one({
            'taskName': taskName, 
            "timestamp": timestamp
        })
        data_list = task_argument_doc['dataList']
        return data_list


@dataclass
class MlModel:
    reg_date: dt = dt.now()
    model_name: str = ""
    model_features: List = field(default_factory=list)
    train_code_list: List = field(default_factory=list)
    test_code_list: List = field(default_factory=list)
    model_params: Dict = field(default_factory=dict)
    model_performance: Dict = field(default_factory=dict)
    report_type: str = ""

    @property
    def to_json(self):
        return asdict(self)

    def save(self, col: Collection):
        data = asdict(self)
        col.insert_one(data)