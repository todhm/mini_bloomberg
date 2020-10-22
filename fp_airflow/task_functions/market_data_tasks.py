from datetime import datetime as dt
from datetime import timedelta
from pendulum import Pendulum
from pymongo import MongoClient, DESCENDING
from dataclass_models.models import TaskArgumentsList
from utils.marketdatahandler import save_api_market_data
import fp_types
import pandas as pd
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
        if start_date >= dt.now():
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
