from airflow import DAG,settings
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pprint
import time 
import pendulum
from dag_utils.dag_function import 
import re 

local_tz = pendulum.timezone("Asia/Seoul")
thisYear = datetime.now().year 
thisMonth = datetime.now().month
thisDate = datetime.now().day + 1

default_args = {
    'owner': 'fidel',
    'depends_on_past': False,
    'start_date': datetime(thisYear, thisMonth, thisDate, hour=4,minute=0,tzinfo=local_tz),
    'schedule_interval':'0 17 * * *',
    'email': ['fidel@crunchprice.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'provide_context':True
}

dag_name = 'glovis_example'
dag = DAG(dag_name,schedule_interval='0 17 * * *',dagrun_timeout=timedelta(hours=10),default_args=default_args,concurrency=8, catchup=False)


crawl_glovis_last_page = PythonOperator(
    task_id='crawl_glovis_last_page',
    python_callable=crawl_glovis_last_page,
    dag=dag,
    op_kwargs={
        'loc':'yangsan' 
    }
)

crawl_glovis_pages = PythonOperator(
    task_id='crawl_glovis_page',
    python_callable=crawl_glovis_page,
    dag=dag
)

upload_glovis_spread_task = PythonOperator(
    task_id='upload_glovis_spread',
    python_callable=upload_glovis_spread,
    dag=dag
)

crawl_glovis_pages.set_upstream(crawl_glovis_last_page)
upload_glovis_spread_task.set_upstream(crawl_glovis_pages)