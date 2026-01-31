import os
import requests
import logging
from datetime import datetime
from google.cloud import storage
from google.api_core.exceptions import Conflict

# -- LOGIN CONFIGURATION --
os.makedirs("logs/ingestion_logs/", exist_ok=True)
log_filename = f"logs/ingestion_logs/ingestion_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename), # Écrit dans le fichier
        logging.StreamHandler()            # Affiche dans le terminal
    ]
)



# -- PROJECT CONFIGURATION --
GCP_KEY_PATH = "gcp_key.json"
DATA_URL = "http://data.insideairbnb.com/france/nouvelle-aquitaine/bordeaux/2025-09-18/visualisations/listings.csv"
BUCKET_NAME = "airbnb-data-engineering-raw-cg"
LOCAL_FILE = "../data/listings_bdx.csv"
GCS_PATH = "raw/listings_bdx.csv"

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

    if not os.path.exists(GCP_KEY_PATH):
        logging.error(f"Key '{GCP_KEY_PATH}' not found ! Ingestion STOP.")
        return

    logging.info(f"Loading Data from {DATA_URL}...")
    try:
        response = requests.get(DATA_URL, stream=True)
        response.raise_for_status()

        os.makedirs("../data", exist_ok=True)
        with open(LOCAL_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info("Dowload execute with succes !")
    except Exception as e:
            logging.error(f"Dowload Error : {e}")
    
    try:
        logging.info("Connexion to Google Cloud Storage...")
        client = storage.Client.from_service_account_json(GCP_KEY_PATH)
        
        try:
            bucket = client.create_bucket(BUCKET_NAME, location='EU')
            logging.info(f"Bucket {BUCKET_NAME} created.")
        except Conflict:
            bucket = client.get_bucket(BUCKET_NAME)
            logging.warning(f"This bucket {BUCKET_NAME} already exist.")
        
        logging.info(f"Send file to gs://{BUCKET_NAME}/{GCS_PATH}...")
        blob = bucket.blob(GCS_PATH)
        blob.upload_from_filename(LOCAL_FILE)

        logging.info(f"SUCCESS: Pipeline ended without error !")
   
    except Exception as e:
        logging.critical(f"CRITICAL GCP ERROR: {e}")

if __name__ == '__main__':
    upload_to_gcs()
        
