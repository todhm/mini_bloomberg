from pymongo import MongoClient
from task_connector.dart_airflow_connector import DartAirflowConnector
from utils.celery_utils import execute_celery_tasks
import config


def create_machine_learning_features(
    db_name=config.TestSettings.MONGODB_NAME, 
    start_idx=0,
    total_task_count=1,
    **kwargs
):
    execution_date = kwargs.get('execution_date')
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
    for idx, data in enumerate(data_list):
        print(idx, data['company'], data['code'])
        code = str(data['code'])
        celery_data = {
            'db_name': db_name,
            'code': code
        }
        try:
            result = execute_celery_tasks(
                taskFunc='save_machinelearing_features_data',
                data=celery_data
            )
            print(data['company'], code, result)
        except Exception as e:
            print("Error occured", e, data['company'], code)
            