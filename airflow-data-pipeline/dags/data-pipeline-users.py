import os, re
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task

from modules.tasks import get_files, extract, transform, load


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example']
)
def data_pipeline_users():

    files = get_files()
    dataframes = extract(files, match_re="user.*.csv")
    transformed = transform.expand(x=dataframes)
    results = load.partial(table_name="inbound_users").expand(x=transformed)
    

    

data_pipeline_users_dag = data_pipeline_users()