from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from task_functions import handle_dart_jobs, create_feature_tasks
from task_functions.market_data_tasks import (
    create_market_data_tasks,
    prepare_recent_market_tasks
)
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
         'db_name': ProductionSettings.MONGODB_NAME,
        },
    )

total_number = 2
save_report_job_list = []
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
            'db_name': ProductionSettings.MONGODB_NAME,
            'only_recents': True,
        },
    )
    save_report_links.set_upstream(company_prepare_tasks)
    save_report_job = PythonOperator(
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
    save_report_job.set_upstream(save_report_links)
    save_report_job_list.append(save_report_job)


market_prepare_job = PythonOperator(
    task_id='prepare_insert_data',
    python_callable=prepare_recent_market_tasks,
    dag=dag,
    depends_on_past=False,
    provide_context=True,
    op_kwargs={
        'db_name': 'fp_data'
    },
)
for srj in save_report_job_list:
    srj.set_upstream(market_prepare_job)

total_number = 15
market_job_list = []
for i in range(total_number):
    save_market_jobs = PythonOperator(
        task_id=f'save_market_data_{i}',
        python_callable=create_market_data_tasks,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'start_idx': i,
         'total_task_count': total_number,
         'db_name': 'fp_data'
        },
    )
    save_market_jobs.set_upstream(market_prepare_job)
    market_job_list.append(save_market_jobs)

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
for mj in market_job_list:
    feature_tasks.set_upstream(mj)