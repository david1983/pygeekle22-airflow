from datetime import datetime, timedelta
from airflow.decorators import dag
from modules.tasks import get_files, extract, transform_revenue, load

default_arguments = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# define the DAG for revenue data pipeline

@dag(
    default_args=default_arguments,
    start_date=datetime(2022, 8, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
)
def revenue_pipeline():
    files = get_files()
    extract_data = extract(files, match_re=".*revenue.*")
    transform_data = transform_revenue.expand(x=extract_data)
    load_data = load.partial(table_name="revenue").expand(x=transform_data)

revenue_dag = revenue_pipeline()