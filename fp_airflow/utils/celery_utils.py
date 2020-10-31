from typing import Dict
import config
import requests
import time


def execute_celery_tasks(
    taskFunc: str = "",
    data: Dict = {},
    max_counts: int = 10000,
    tick_time: int = 2.5
) -> Dict:
    post_data = {
        "taskFunc": taskFunc,
        "data": data
    }
    connector_url = config.BaseSettings.CONNECTOR_URI
    response = requests.post(
        f"{connector_url}/launch_celery_tasks", 
        json=post_data
    )
    if response.status_code != 200:
        print(response.text)
        raise ValueError("Error while making celery tasks")
    
    task_data = response.json()
    task_id = task_data['taskId']
    task_url = f"{connector_url}/tasks/{task_id}"
    start_count = 0
    while start_count < max_counts:
        response = requests.get(task_url)
        if response.status_code != 200:
            print(response.text)
            raise ValueError("Error while making task fetch requests")
        json_result = response.json()
        if json_result['state'] in ['PENDING', 'RECEIVED', 'STARTED', 'RETRY']:
            time.sleep(tick_time)
            continue
        if json_result['state'] in [
            'FAILURE', 'REVOKED', "REJECTED", 'IGNORED'
        ]:
            raise ValueError("Error occured  inside celery jobs")
        if json_result['state'] in ['SUCCESS']:
            return json_result['data']
    raise ValueError("Failed")

        



    




