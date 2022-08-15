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
def data_pipeline_all():    
    files = get_files()
    users_dfs = extract.override(task_id="extract_users")(files, match_re="user.*.csv")
    users_transformed = transform.override(task_id="transform_users").expand(x=users_dfs)
    users_results = load.override(task_id="load_users").partial(table_name="inbound_users").expand(x=users_transformed)
    revenues_dfs = extract.override(task_id="extract_revenues")(files, match_re="revenue.*.csv")
    revenues_transformed = transform.override(task_id="transform_revevenues").expand(x=revenues_dfs)
    revenues_results = load.override(task_id="load_revenues").partial(table_name="inbound_revenues").expand(x=revenues_transformed)
    

data_pipeline_all_dag = data_pipeline_all()