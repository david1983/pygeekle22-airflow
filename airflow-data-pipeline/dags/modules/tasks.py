import os, re
import pandas as pd
from airflow.decorators import task
from modules.settings import INBOUND_DIR, DB_USER, DB_PASS, DB_HOST, DB_NAME, DB_PORT
from sqlalchemy import create_engine
from dateutil.parser import parse

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

@task()
def get_files():
    return os.listdir(INBOUND_DIR)

@task()    
def extract(files: str, match_re=".*", **kwargs):    
    execution_date = kwargs.get("execution_date")
    dataframes = []
    files = [file for file in files if re.match(match_re, file)]
    d = execution_date.strftime("%Y-%m-%d")
    files = [file for file in files if re.match(f".*{d}.csv", file)]
    for file in files:
        path = os.path.join(INBOUND_DIR, file)
        df = pd.read_csv(path)
        dataframes.append(df.to_json())
    return dataframes

@task()
def transform(x):
    df = pd.read_json(x)
    # apply some data transformations here
    return df.to_json()

@task()
def load(x, table_name=""):    
    if table_name == "":
        return
    df = pd.read_json(x)
    df.to_sql(table_name, engine, if_exists='append', chunksize=1000)
    print(f"synced {len(df)} records in table {table_name}")    