from datetime import datetime, timedelta
from airflow.decorators import dag
from modules.tasks import get_files, extract, transform, transform_revenue, load

default_arguments = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# define the DAG for revenue data pipeline