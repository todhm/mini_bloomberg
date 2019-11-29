import requests
import json 
import time 
from airflow import AirflowException
import os 


def call_async_func(url,get_url,data):
    response = requests.post(url,data=data)
    response = json.loads(response.text)
    task_id = response['taskId']
    get_url += task_id
    while True: 
        time.sleep(3)
        res = requests.get(get_url)
        result_data = json.loads(res.text)
        if result_data.get('complete') ==True or res.status_code != 200: 
            break 
    if result_data.get("result") =="failed":
        raise AirflowException

    return result_data
    
     