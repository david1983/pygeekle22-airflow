import os, re
import pandas as pd
from modules.setting import DB_URI, INBOUND_DIR
from airflow.decorators import task
from sqlalchemy import create_engine

engine = create_engine(DB_URI)

# define the tasks and utility functions used in the DAGs

@task()
def get_files():
    """
    Get the files in the inbound directory
    """
    files = os.listdir(INBOUND_DIR)
    return files

@task()
def extract(files, match_re=".*", **kwargs):
    """
    Extract the data from the files matching the regex
    """
    execution_date = kwargs["execution_date"]
    matching_files = [file for file in files if re.match(match_re, file)]
    d = execution_date
    matching_date = [file for file in matching_files if d.strftime("%Y-%m-%d") in file]
    
    dataframes = [pd.read_csv(os.path.join(INBOUND_DIR, file)) for file in matching_date]
    dataframes_json = [df.to_json() for df in dataframes]
    return dataframes_json

@task()
def transform(x):
    """ mock transform"""
    df = pd.read_json(x)
    return df.to_json()

@task()
def load(x, table_name):
    if not table_name: 
        return
    df = pd.read_json(x)
    df.to_sql(table_name, engine, if_exists="append", index=False)
    return True

def transform_revenue_fn(x):
    df = pd.read_json(x)
    df["price_cents"] = df["price"] * 100
    return df.to_json()

@task()
def transform_revenue(x):
    return transform_revenue_fn(x)