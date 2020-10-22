FROM puckel/docker-airflow:1.10.9

COPY . /usr/local/airflow/dags
COPY ./requirements.txt /requirements.txt
USER root
RUN pip install -r /requirements.txt
USER airflow