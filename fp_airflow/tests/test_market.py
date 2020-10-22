from task_functions import market_data_tasks
from pendulum import Pendulum
from dataclass_models.models import TaskArgumentsList
from utils.test_utils import create_market_data
from datetime import datetime as dt, timedelta
import fp_types
import pandas as pd


def test_market_tasks(mongodb):
    db = mongodb
    execution_date = Pendulum.now()
    timestamp = int(execution_date.timestamp())
    date_list = (
        pd.date_range(
            start="2018-08-01",
            end="2018-08-14"
        ).to_pydatetime().tolist()
    )
    tal = TaskArgumentsList(
        timestamp=timestamp,
        dataList=date_list,
        taskName=fp_types.MARKET_DATA_FETCH
    )
    tal.save(db)
    market_data_tasks.create_market_data_tasks(
        start_idx=0,
        total_task_count=2,
        execution_date=execution_date,
        db_name='testdb',
    )
    market_data_tasks.create_market_data_tasks(
        start_idx=1,
        total_task_count=2,
        execution_date=execution_date,
        db_name='testdb',
    )
    for date in date_list:
        if date.weekday() != 5 and date.weekday() != 6:
            data_count = db.market_data.find({"Date": date}).count()
            assert data_count > 0


def test_save_market_crawling_dates(mongodb):
    db = mongodb
    execution_date = Pendulum.now()
    timestamp = int(execution_date.timestamp())
    market_data_tasks.prepare_market_tasks(
        execution_date=Pendulum.now(),
        db_name='testdb',
    )
    task_instance = db.airflow_task_list.find_one(
        {"timestamp": timestamp}
    )
    assert task_instance['taskName'] == fp_types.MARKET_DATA_FETCH
    assert task_instance['timestamp'] == timestamp
    data_list = task_instance['dataList']
    first_data = data_list[0]
    last_data = data_list[-1]
    assert dt.strftime(first_data, "%Y-%m-%d") == '1999-01-01'
    assert(
        dt.strftime(last_data, "%Y-%m-%d") 
        == dt.strftime(dt.now(), "%Y-%m-%d")
    )
    

def test_save_market_crawling_with_inserted_db(mongodb):
    db = mongodb
    execution_date = Pendulum.now()
    timestamp = int(execution_date.timestamp())
    for i in range(3, 10):
        date = dt.now() - timedelta(days=i)
        market_data = create_market_data(Date=date)
        db.market_data.insert_one(market_data)
    market_data_tasks.prepare_market_tasks(
        execution_date=Pendulum.now(),
        db_name='testdb',
    )
    task_instance = db.airflow_task_list.find_one(
        {"timestamp": timestamp}
    )
    assert task_instance['taskName'] == fp_types.MARKET_DATA_FETCH
    assert task_instance['timestamp'] == timestamp
    data_list = task_instance['dataList']
    assert len(data_list) == 3
    for i in range(0, 3):
        delta_days = len(data_list) - i - 1
        date = dt.now() - timedelta(days=delta_days)
        assert (
            dt.strftime(date, "%Y-%m-%d") == 
            dt.strftime(data_list[i], "%Y-%m-%d") 
        )
        
    