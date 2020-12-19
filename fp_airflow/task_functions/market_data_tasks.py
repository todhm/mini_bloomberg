from datetime import datetime as dt
from datetime import timedelta
from pendulum import Pendulum
from pymongo import MongoClient, DESCENDING
import pandas as pd
from workalendar.asia import SouthKorea
from dataclass_models.models import TaskArgumentsList
from utils.marketdatahandler import save_api_market_data
from fp_common import fp_types
import os


def prepare_market_tasks(
    execution_date: Pendulum = Pendulum.now(),
    db_name: str = 'testdb',
    **kwargs
):
    timestamp = int(execution_date.timestamp())
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client[db_name]
    market_data = list(
        db.market_data
        .find({}, {"Date": 1})
        .sort('Date', DESCENDING)
        .limit(1)
    )
    if market_data:
        start_date = market_data[0]['Date'] 
        today_date = dt.strptime(dt.strftime(dt.now(), '%Y%m%d'), '%Y%m%d')
        if start_date >= today_date:
            return True
        start_date += timedelta(days=1)
        start_date = dt.strftime(start_date, '%Y-%m-%d')
    else:
        start_date = "1999-01-01"

    date_list = (
        pd.date_range(
            start=start_date,
            end=dt.strftime(dt.now(), '%Y-%m-%d')
        ).to_pydatetime().tolist()
    )
    ta = TaskArgumentsList(
        timestamp=timestamp, 
        dataList=date_list,
        taskName=fp_types.MARKET_DATA_FETCH
    )
    ta.save(db)
    client.close()


def create_market_data_tasks(
    start_idx: int = 0, 
    total_task_count: int = 1, 
    execution_date: Pendulum = Pendulum.now(),
    db_name: str = 'testdb',
    **kwargs
):
    timestamp = int(execution_date.timestamp())
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client[db_name]
    data_list = TaskArgumentsList.fetch_current_args(
        db,
        fp_types.MARKET_DATA_FETCH, 
        timestamp,
        start_idx, 
        total_task_count
    )
    print(len(data_list))
    save_api_market_data(data_list, db.market_data)


def prepare_recent_market_tasks(
    execution_date: Pendulum = Pendulum.now(),
    db_name: str = 'testdb',
    **kwargs
):
    timestamp = int(execution_date.timestamp())
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client[db_name]
    market_data = list(
        db.market_data
        .find({}, {"Date": 1})
        .sort('Date', DESCENDING)
        .limit(1)
    )
    nowdate = dt.now()
    ca = SouthKorea()
    start_working_days = ca.add_working_days(nowdate, -11)
    if market_data:
        start_date = market_data[0]['Date'] 
        today_date = dt.strptime(dt.strftime(dt.now(), '%Y%m%d'), '%Y%m%d')
        if start_date >= today_date:
            return True
        start_date = max(
            dt.strptime(
                start_working_days.strftime('%Y%m%d'),
                '%Y%m%d'
            ), 
            start_date
        )
    else:
        start_date = start_working_days
    start_date = dt.strftime(start_date, '%Y-%m-%d')
    date_list = (
        pd.date_range(
            start=start_date,
            end=dt.strftime(dt.now(), '%Y-%m-%d')
        ).to_pydatetime().tolist()
    )
    ta = TaskArgumentsList(
        timestamp=timestamp, 
        dataList=date_list,
        taskName=fp_types.MARKET_DATA_FETCH
    )
    ta.save(db)
    client.close()
