from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from dag_utils.dag_function import handle_dart_jobs
from datetime import datetime, timedelta
import pprint
import time 
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")
thisYear = datetime.now().year -1
thisMonth = datetime.now().month
thisDate = datetime.now().day
thisDate = 1 if thisDate ==31 else thisDate

default_args = {
    'owner': 'Fidel',
    'depends_on_past': False,
    'start_date': datetime(thisYear, thisMonth, thisDate, tzinfo=local_tz),
    'email': ['gmlaud14@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': timedelta(seconds=5),
    'wait_for_downstream':False,
    'provide_context':True

}

dag = DAG('dart_dag',default_args=default_args,schedule_interval=None,concurrency=5,catchup=False)


total_number = 10
for i in range(total_number):
    prepare_alli_delivery = PythonOperator(
        task_id=f'save_report_data_{i}',
        python_callable=handle_dart_jobs,
        dag=dag,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
         'index': i,
         'total_number':total_number
        },
    )