from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from task_functions import ml_models_tasks
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
    'simulation_dags', 
    default_args=default_args, 
    schedule_interval=None, 
    concurrency=15, 
    catchup=False
)


create_model_tasks = PythonOperator(
    task_id='create_ml_models',
    python_callable=ml_models_tasks.create_machine_learning_models,
    dag=dag,
    depends_on_past=False,
    provide_context=True,
    op_kwargs={
        'db_name': 'fp_data',
        'model_name': 'randomforest'
    },
)

total_number = 2
for i in range(total_number):
    save_simulation_tasks = PythonOperator(
        task_id=f'simulate_with_model_tasks_{i}',
        python_callable=ml_models_tasks.create_simulation_results,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'start_idx': i,
         'total_task_count': total_number,
         'db_name': 'fp_data'
        },
    )
    save_simulation_tasks.set_upstream(create_model_tasks)