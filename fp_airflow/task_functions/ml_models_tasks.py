import os
from pymongo import MongoClient
from utils.celery_utils import execute_celery_tasks
from dataclass_models.models import TaskArgumentsList
import config
import fp_types


def create_machine_learning_models(
    db_name=config.TestSettings.MONGODB_NAME, 
    model_name: str = 'test_random_forest',
    **kwargs
):
    execution_date = kwargs.get('execution_date')
    ts = execution_date.timestamp()
    ts = int(ts)
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client[db_name]
    print("current timestamp", ts)
    celery_data = {
        'db_name': db_name,
        'model_name': model_name
    }
    try:
        result = execute_celery_tasks(
            taskFunc='save_ml_models',
            data=celery_data
        )
        print(result)
    except Exception as e:
        print("Error occured", e)
        raise e
        
    ta = TaskArgumentsList(
        timestamp=ts, 
        dataList=result,
        taskName=fp_types.SIMULATE_WITH_ML_MODELS
    )
    ta.save(db)
    client.close()


def create_simulation_results(
    db_name=config.TestSettings.MONGODB_NAME, 
    start_idx=0,
    total_task_count=1,
    **kwargs
):
    execution_date = kwargs.get('execution_date')
    ts = execution_date.timestamp()
    ts = int(ts)
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client[db_name]
    print("current timestamp", ts)
    data_list = TaskArgumentsList.fetch_current_args(
        db,
        fp_types.SIMULATE_WITH_ML_MODELS, 
        ts,
        start_idx, 
        total_task_count
    )
    for data in data_list:
        celery_data = {
            'db_name': db_name,
            'model_name': data['model_name']
        }
        try:
            result = execute_celery_tasks(
                taskFunc='simulate_model_result',
                data=celery_data
            )
            print(result)
        except Exception as e:
            print("Error occured", e)
            raise e
    client.close()