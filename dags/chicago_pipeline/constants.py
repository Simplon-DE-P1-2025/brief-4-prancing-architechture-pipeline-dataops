from datetime import timedelta

DAG_DOC_MD = """
# Chicago Crimes Pipeline Multi API

Pipeline ETL unique organise en trois groupes:

1. `ingestion`
   Extraction API paginee, validation Soda du brut, publication des jeux `valid` et `quarantine`.
2. `transformation`
   Nettoyage, agregations, validation Soda puis separation du dataset prepare.
3. `loading`
   Preparation des tables PostgreSQL puis chargement final des jeux valid et quarantine.

## Sorties locales

- `include/data/raw/`
- `include/data/processed/`
- `include/data/quarantine/`
- `include/data/reports/`

## Regles qualite

- Contrat raw: `include/soda/contracts/raw_contract.yml`
- Contrat processed: `include/soda/contracts/processed_contract.yml`

## Sequence processed

1. `validate_processed_contract`
2. `split_processed_outputs`
3. `publish_valid_processed`
4. `publish_quarantine_processed`
5. `create_target_tables`
6. `load_valid_records`
7. `load_quarantine_records`
"""

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

AIRFLOW_DATA_DIR = "/usr/local/airflow/include/data"
RAW_EXTRACTED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/raw/chicago_crimes_raw_extracted.csv"
RAW_CSV_PATH = f"{AIRFLOW_DATA_DIR}/raw/chicago_crimes_raw.csv"
FILTERED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_filtered.csv"
AGGREGATED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_aggregated.csv"
AGG_HOURLY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_agg_hourly.csv"
AGG_MONTHLY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_agg_monthly.csv"
AGG_SERIOUS_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_agg_serious.csv"
AGG_COMMUNITY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_agg_community.csv"
AGG_YEARLY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_agg_yearly.csv"
CLEAN_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_clean.csv"
PROCESSED_VALID_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_valid.csv"
RAW_QUARANTINE_CSV_PATH = f"{AIRFLOW_DATA_DIR}/quarantine/chicago_crimes_raw_quarantine.csv"
PROCESSED_QUARANTINE_CSV_PATH = (
    f"{AIRFLOW_DATA_DIR}/quarantine/chicago_crimes_processed_quarantine.csv"
)
QUARANTINE_CSV_PATH = f"{AIRFLOW_DATA_DIR}/quarantine/chicago_crimes_quarantine.csv"
REPORTS_DIR = f"{AIRFLOW_DATA_DIR}/reports"

TARGET_DB = "chicago_crimes"
CONN_ID = "postgres_default"
SODA_CONFIG_PATH = "/usr/local/airflow/include/soda/configuration.yml"
RAW_CONTRACT_PATH = "/usr/local/airflow/include/soda/contracts/raw_contract.yml"
PROCESSED_CONTRACT_PATH = "/usr/local/airflow/include/soda/contracts/processed_contract.yml"

RAW_CONTRACT_TABLE = "chicago_crimes_raw_contract"
PROCESSED_CONTRACT_TABLE = "chicago_crimes_processed_contract"
RAW_QUARANTINE_TABLE = "chicago_crimes_raw_quarantine"
PROCESSED_VALID_TABLE = "chicago_crimes_processed_valid"
PROCESSED_QUARANTINE_TABLE = "chicago_crimes_processed_quarantine"

PAGE_SIZE = 1000

FIELDNAMES = [
    "id",
    "case_number",
    "date",
    "block",
    "iucr",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "beat",
    "district",
    "ward",
    "community_area",
    "fbi_code",
    "x_coordinate",
    "y_coordinate",
    "year",
    "updated_on",
    "latitude",
    "longitude",
]
