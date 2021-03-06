from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pendulum

from config import ProductionSettings
from task_functions import handle_dart_jobs, create_feature_tasks
from task_functions.market_data_tasks import (
    create_market_data_tasks,
    prepare_recent_market_tasks
)
from task_functions.create_recommendation_tasks import (
    create_recommendation
)


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
    'make_daily_recommendations', 
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

total_number = 10
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
    market_prepare_job.set_upstream(srj)

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

dummy_job = DummyOperator(
    task_id='dummyjobfirst',
    dag=dag,
)
for market_job in market_job_list:
    dummy_job.set_upstream(market_job)

feature_task_list = []
for i in range(3):
    feature_tasks = PythonOperator(
        task_id=f'create_ml_features_{i}',
        python_callable=create_feature_tasks.create_machine_learning_features,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            'start_idx': i,
            'total_task_count': total_number,
            'db_name': ProductionSettings.MONGODB_NAME,
            'use_current_date': True
        },
    )
    feature_tasks.set_upstream(dummy_job)
    feature_task_list.append(feature_tasks)

second_dummy_job = DummyOperator(
    task_id='second_dummy_tasks',
    dag=dag,
)
for ft in feature_task_list:
    second_dummy_job.set_upstream(ft)

portfolio_id = Variable.get('portfolio_id')
recommend_jobs = PythonOperator(
        task_id='create_recommendations',
        python_callable=create_recommendation,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            'portfolio_id': portfolio_id,
            'db_name': ProductionSettings.MONGODB_NAME,
        },
    )
recommend_jobs.set_upstream(second_dummy_job)