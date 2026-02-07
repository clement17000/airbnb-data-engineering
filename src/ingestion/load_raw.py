import os
import sys
import requests
import logging
from datetime import datetime
from google.cloud import storage
from google.api_core.exceptions import Conflict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config

# -- LOGIN CONFIGURATION --
logging = config.setup_logging("ingestion")



# -- PROJECT CONFIGURATION --
local_file_path = config.DATA_DIR / "listings_bdx.csv"
gcs_path = "raw/listings_bdx.csv"

def upload_to_gcs():
    """
    Downloads Airbnb data from the web and sends it to GCS.
    
    This function performs the following steps:
    1. Downloads the file in chunks.
    2. Connects to GCP via the JSON key.
    3. Uploads the file to the specified bucket.
    
    Raises:
        Exception: If the download or upload fails.
    """
    logging.info("Starting injection pipeline.")

    if not os.path.exists(config.GCP_KEY_PATH):
        logging.error(f"Key '{config.GCP_KEY_PATH}' not found ! Ingestion STOP.")
        return

    logging.info(f"Loading Data from {config.DATA_URL}...")
    try:
        response = requests.get(config.DATA_URL, stream=True)
        response.raise_for_status()

        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(local_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info("Dowload execute with succes !")
    except Exception as e:
            logging.error(f"Dowload Error : {e}")
            return
    
    try:
        logging.info("Connexion to Google Cloud Storage...")
        client = storage.Client()
        
        try:
            bucket = client.create_bucket(config.BUCKET_NAME, location='EU')
            logging.info(f"Bucket {config.BUCKET_NAME} created.")
        except Conflict:
            bucket = client.get_bucket(config.BUCKET_NAME)
            logging.warning(f"This bucket {config.BUCKET_NAME} already exist.")
        
        logging.info(f"Send file to gs://{config.BUCKET_NAME}/{gcs_path}...")
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file_path)

        logging.info(f"SUCCESS: Pipeline ended without error !")
   
    except Exception as e:
        logging.critical(f"CRITICAL GCP ERROR: {e}")

if __name__ == '__main__':
    upload_to_gcs()
        
