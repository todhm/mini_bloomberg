from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from task_functions import handle_dart_jobs
from task_functions import create_feature_tasks
from datetime import datetime, timedelta
from config import ProductionSettings
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
    'dart_dag', 
    default_args=default_args, 
    schedule_interval=None, 
    concurrency=15, 
    catchup=False
)


company_prepare_tasks = PythonOperator(
        task_id='company_prepare_tasks',
        python_callable=handle_dart_jobs.prepare_company_report_list,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'db_name': ProductionSettings.MONGODB_NAME
        },
    )

total_number = 2
for i in range(total_number):
    save_report_links = PythonOperator(
        task_id=f'save_report_links_{i}',
        python_callable=handle_dart_jobs.prepare_company_links,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'start_idx': i,
         'total_task_count': total_number,
         'db_name': ProductionSettings.MONGODB_NAME
        },
    )
    save_report_links.set_upstream(company_prepare_tasks)
    save_report_data = PythonOperator(
        task_id=f'save_report_data_{i}',
        python_callable=handle_dart_jobs.insert_company_data_list,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'start_idx': i,
         'total_task_count': total_number,
         'db_name': ProductionSettings.MONGODB_NAME
        },
    )
    save_report_data.set_upstream(save_report_links)
    feature_tasks = PythonOperator(
        task_id=f'create_ml_features_{i}',
        python_callable=create_feature_tasks.create_machine_learning_features,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'start_idx': i,
         'total_task_count': total_number,
         'db_name': ProductionSettings.MONGODB_NAME
        },
    )
    feature_tasks.set_upstream(save_report_data)