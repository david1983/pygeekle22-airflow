from datetime import datetime, timedelta
from airflow.decorators import dag
from modules.tasks import get_files, extract, transform, load

default_arguments = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# define the DAG for user data pipeline
@dag(
    default_args=default_arguments,
    start_date=datetime(2022, 8, 1),
    schedule_interval=timedelta(days=1),
    catchup=True,
)
def user_pipeline():
    files = get_files()
    extract_data = extract(files, match_re=".*user.*")
    transform_data = transform.expand(x=extract_data)
    load_data = load.partial(table_name="user").expand(x=transform_data)

user_dag = user_pipeline()