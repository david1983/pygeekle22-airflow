import logging
import os
import argh
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "pass")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_PORT = os.getenv("DB_PORT", "5435")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def sync(inbound_directory: str = "", table_name: str = "revenue"):
    """
    sync the files landing on the inbound directory with the database
    """
    logging.info(f"syncing folder {inbound_directory}")
    for file in os.listdir(inbound_directory):       
        logger.info(f"syncing file {file}") 
        df = pd.read_csv(f"{inbound_directory}/{file}")        
        df.to_sql(table_name, engine, if_exists='append', chunksize=1000)
        logger.info(f"synced {len(df)} records in table {table_name}")


# assembling:

parser = argh.ArghParser()
parser.add_commands([sync])

# dispatching:

if __name__ == "__main__":
    parser.dispatch()
