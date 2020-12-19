from pendulum import Pendulum

from utils.celery_utils import execute_celery_tasks
import config


def create_recommendation(
    execution_date: Pendulum,
    db_name=config.TestSettings.MONGODB_NAME, 
    portfolio_id: str = '',
    **kwargs
):
    ts = execution_date.timestamp()
    ts = int(ts)
    print("current timestamp", ts)
    current_date_string = execution_date.strftime('%Y%m%d')
    celery_data = {
        'db_name': db_name,
        'stock_date': current_date_string,
        'portfolio_id': portfolio_id
    }
    result = execute_celery_tasks(
        taskFunc='save_recommendation',
        tick_time=4.0,
        data=celery_data
    )
    print(result)
        