import os, re
import pandas as pd
from modules.setting import DB_URI, INBOUND_DIR
from airflow.decorators import task
from modules.setting import DB_URI, INBOUND_DIR
from sqlalchemy import create_engine

engine = create_engine(DB_URI)

@task()
def get_files():
    return os.listdir(INBOUND_DIR)

@task()
def extract(files, match_re=".*", **kwargs):
    execution_date = kwargs["execution_date"]
    dataframes = []
    files = [file for file in files if re.match(match_re, file)]
    d = execution_date.strftime("%Y-%m-%d")
    files = [file for file in files if re.match(f".*{d}.*.csv", file)]
    for file in files:
        path = os.path.join(INBOUND_DIR, file)
        df = pd.read_csv(path)
        dataframes.append(df.to_json())
    return dataframes

@task()
def transform(x):
    df = pd.read_json(x)
    # apply a transformation here
    return df.to_json()


def transform_revenue_fn(x):
    df = pd.read_json(x)
    df["price_in_cents"] = df["price"] * 100    
    return df.to_json()

@task()
def transform_revenue(x):
    return transform_revenue_fn(x)


@task()
def load(x, table_name):
    df = pd.read_json(x)
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"synced {table_name} with {len(df)} rows")
