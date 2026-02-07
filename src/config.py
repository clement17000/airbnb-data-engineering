import os
import logging
from datetime import datetime
from pathlib import Path  # <--- L'outil magique

# --- 1. GESTION DES CHEMINS (PATHLIB) ---
# On récupère le chemin absolu du fichier config.py actuel
# .parent = dossier 'src'
# .parent.parent = la racine du projet 'airbnb-project'
PROJECT_ROOT = Path(__file__).parent.parent 

# On définit les dossiers clés par rapport à la racine
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
SRC_DIR = PROJECT_ROOT / "src"

SQL_DIR = SRC_DIR / "sql" 

# On définit les fichiers spécifiques
GCP_KEY_PATH = SRC_DIR / "gcp_key.json"

SQL_INIT_TABLE_FILE = SQL_DIR / "create_tables.sql"

# --- 2. CONSTANTES GLOBALES ---
BUCKET_NAME = "airbnb-data-engineering-raw-cg"
DATA_URL = "http://data.insideairbnb.com/france/nouvelle-aquitaine/bordeaux/2025-09-18/visualisations/listings.csv"

# --- 3. AUTHENTIFICATION ---
# On convertit le Path en string car os.environ attend du texte
if GCP_KEY_PATH.exists():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(GCP_KEY_PATH)
else:
    print(f"ATTENTION: Clé introuvable à : {GCP_KEY_PATH}")

# --- 4. LOGGING ---
def setup_logging(script_name):
    # On utilise le chemin dynamique
    specific_log_dir = LOGS_DIR / f"{script_name}_logs"
    specific_log_dir.mkdir(parents=True, exist_ok=True)

    log_filename = specific_log_dir / f"{script_name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ],
        force=True
    )
    return logging.getLogger()