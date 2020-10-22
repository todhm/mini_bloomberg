import os
# Celery configuration
# http://docs.celeryproject.org/en/latest/configuration.html

broker_url = os.environ.get("REDIS_URL")
result_backend = os.environ.get("REDIS_URL")

# json serializer is more secure than the default pickle
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'

# Use UTC instead of localtime
timezone = 'Asia/Seoul'

# Maximum retries per task
task_track_started = True
imports = ('pipeline.tasks', )

# A custom property used in tasks.py:run()