import os
import shutil
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

inbound_dir = "/home/d4v3/dev/pygeekle22-airflow/simple-data-pipeline/inbound"
process_dir = "/home/d4v3/dev/pygeekle22-airflow/simple-data-pipeline/processed"

engine = create_engine("postgresql://user:pass@localhost:5435/postgres")

# create a script to read files from the inbound directory 
# load the content of the file in a db table 
# and move them to the processed directory when it is done
def load_file(file_path):
    logger.info(f"Loading file {file_path}")
    df = pd.read_csv(file_path)
    df.to_sql("revenue", engine, if_exists="append", index=False)
    shutil.move(file_path, process_dir)
    logger.info(f"File {file_path} loaded")

for file in os.listdir(inbound_dir):
    load_file(os.path.join(inbound_dir, file))