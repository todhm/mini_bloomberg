from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from task_functions import market_data_tasks
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")
thisYear = datetime.now().year - 1
thisMonth = datetime.now().month
thisDate = datetime.now().day
thisDate = 1 if thisDate == 31 else thisDate

default_args = {
    'owner': 'Fidel',
    'depends_on_past': False,
    'start_date': datetime(thisYear, thisMonth, thisDate, tzinfo=local_tz),
    'email': ['gmlaud14@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'wait_for_downstream': False,
    'provide_context': True
}

dag = DAG(
    'market_dags', 
    default_args=default_args, 
    schedule_interval=None, 
    concurrency=15, 
    catchup=False
)


market_prepare_tasks = PythonOperator(
    task_id='prepare_insert_data',
    python_callable=market_data_tasks.prepare_market_tasks,
    dag=dag,
    depends_on_past=False,
    provide_context=True,
    op_kwargs={
        'db_name': 'fp_data'
    },
)

total_number = 15
for i in range(total_number):
    save_market_tasks = PythonOperator(
        task_id=f'save_market_data_{i}',
        python_callable=market_data_tasks.create_market_data_tasks,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'start_idx': i,
         'total_task_count': total_number,
         'db_name': 'fp_data'
        },
    )
    save_market_tasks.set_upstream(market_prepare_tasks)