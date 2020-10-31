from pymongo import MongoClient
from dataclass_models.models import CompanyReport
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
    data_list = CompanyReport.return_company_data(
        db.company_list,
        start_idx,
        total_task_count
    )
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
            