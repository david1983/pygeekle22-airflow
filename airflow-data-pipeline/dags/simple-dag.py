from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# define a simple DAG using only the DummyOperator and PythonOperator

with DAG(
    "simple-dag",
    default_args={},
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2022, 8, 1),
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")
    node1 = PythonOperator(task_id="node1", python_callable=lambda: print("Hello World"))
    node2 = PythonOperator(task_id="node2", python_callable=lambda: print("Hello World"))
    end = DummyOperator(task_id="end")
    start >> node1 >> node2 >> end
