import os
import shutil
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

inbound_dir = "/home/d4v3/dev/pygeekle22-airflow/simple-data-pipeline/inbound"
process_dir = "/home/d4v3/dev/pygeekle22-airflow/simple-data-pipeline/processed"

# create a script to read files from the inbound directory 
# load the content of the file in a db table 
# and move them to the processed directory when it is done
def load_data(**kwargs):
    pass