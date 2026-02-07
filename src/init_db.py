import os
import requests
import logging
from datetime import datetime
from google.cloud import bigquery

import config

# -- LOGIN CONFIGURATION --
logging = config.setup_logging("init_db")

def create_schema_and_tables():
    logging.info("Starting creation of Schema & Tables.")

    try:
        client = bigquery.Client()
        logging.info("Connexion to BigQuery established.")

        with open(config.SQL_INIT_TABLE_FILE, "r") as f:
            sql_query = f.read()
        
        logging.info(f"SQL File read: {config.SQL_INIT_TABLE_FILE}")

        query_job = client.query(sql_query)
        query_job.result()

        logging.info("SUCCES : Dataset & Table has been updated/created.")

    except Exception as e:
        logging.error(f"ERROR : {e}")
        raise e
    
if __name__ == '__main__':
    create_schema_and_tables()