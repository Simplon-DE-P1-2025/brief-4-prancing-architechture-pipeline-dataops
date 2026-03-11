# Configuration partagée entre tous les DAGs du pipeline

API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_BATCH_SIZE = 20000
MAX_ROWS = 2000000  # 2M lignes

RAW_CSV = "/usr/local/airflow/include/data/chicago_crimes_raw.csv"
CLEAN_CSV = "/usr/local/airflow/include/data/chicago_crimes_clean.csv"

SODA_CONFIG = "/usr/local/airflow/include/soda/soda_config.yml"
CHECKS_RAW = "/usr/local/airflow/include/soda/checks_raw.yml"
CHECKS_CLEAN = "/usr/local/airflow/include/soda/checks_clean.yml"

POSTGRES_CONN_ID = "postgres_default"

RAW_COLS = [
    "id", "date", "primary_type", "description",
    "location_description", "arrest", "domestic",
    "district", "ward", "community_area", "year",
    "latitude", "longitude"
]
