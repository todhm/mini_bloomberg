from datetime import datetime as dt

from pendulum import Pendulum
from pymongo import MongoClient
from task_connector.dart_airflow_connector import DartAirflowConnector
from fp_common.fp_utils.time_utils import get_now_time

from utils.celery_utils import execute_celery_tasks
from fp_common import fp_types
import config


def create_machine_learning_features(
    execution_date: Pendulum,
    db_name=config.TestSettings.MONGODB_NAME, 
    start_idx=0,
    total_task_count=1,
    use_current_date=False,
    **kwargs
):
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    mongo_uri = config.BaseSettings.MONGO_URI
    client = MongoClient(mongo_uri)
    db = client[db_name]
    dac = DartAirflowConnector(
        db=db,
        start_idx=start_idx, 
        total_task_count=total_task_count, 
    )
    data_list = dac.return_current_task_companies()
    if use_current_date:
        korean_time = execution_date.in_timezone('Asia/Seoul')
        current_date_string = korean_time.strftime('%Y%m%d')
    for idx, data in enumerate(data_list):
        print(idx, data['company'], data['code'])
        code = str(data['code'])
        for report_type in [
            fp_types.NORMAL_FINANCIAL_STATEMENTS,
            fp_types.CONNECTED_FINANCIAL_STATEMENTS
        ]:
            celery_data = {
                'db_name': db_name,
                'code': code,
                'report_type': report_type
            }
            if use_current_date:
                celery_data['market_date'] = current_date_string
            try:
                result = execute_celery_tasks(
                    taskFunc='save_machinelearing_features_data',
                    data=celery_data
                )
                print(
                    data['company'], 
                    code, 
                    result, 
                    report_type
                )
            except Exception as e:
                print("Error occured", e, data['company'], code, report_type)
                