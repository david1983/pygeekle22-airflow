import os
import shutil
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
engine = create_engine("postgresql://user:pass@localhost:5435/postgres")

inbound_dir = "/home/d4v3/dev/pygeekle22-airflow/simple-data-pipeline/inbound"
processed_dir = "/home/d4v3/dev/pygeekle22-airflow/simple-data-pipeline/processed"

for file in os.listdir(inbound_dir):
    logger.info(f"loading {file}")
    path = os.path.join(inbound_dir, file)
    df = pd.read_csv(path)
    df.to_sql("inbound_revenues", engine, if_exists="append", index=False)
    logger.info(f"loaded {file}")
    shutil.move(path, processed_dir)
