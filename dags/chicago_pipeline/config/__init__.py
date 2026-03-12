from datetime import timedelta
from pathlib import Path

import yaml


def _load_config() -> dict:
    config_path = Path(__file__).with_name("config.yml")
    with config_path.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


_CONFIG = _load_config()

DAG_DOC_MD = _CONFIG["dag_doc_md"]

DEFAULT_ARGS = {
    "owner": _CONFIG["default_args"]["owner"],
    "depends_on_past": _CONFIG["default_args"]["depends_on_past"],
    "retries": _CONFIG["default_args"]["retries"],
    "retry_delay": timedelta(minutes=_CONFIG["default_args"]["retry_delay_minutes"]),
}

AIRFLOW_DATA_DIR = _CONFIG["paths"]["airflow_data_dir"]
RAW_EXTRACTED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['raw_extracted_csv']}"
RAW_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['raw_csv']}"
FILTERED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['filtered_csv']}"
AGGREGATED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['aggregated_csv']}"
AGG_HOURLY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['agg_hourly_csv']}"
AGG_MONTHLY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['agg_monthly_csv']}"
AGG_SERIOUS_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['agg_serious_csv']}"
AGG_COMMUNITY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['agg_community_csv']}"
AGG_YEARLY_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['agg_yearly_csv']}"
CLEAN_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['clean_csv']}"
PROCESSED_VALID_CSV_PATH = (
    f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['processed_valid_csv']}"
)
RAW_QUARANTINE_CSV_PATH = (
    f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['raw_quarantine_csv']}"
)
PROCESSED_QUARANTINE_CSV_PATH = (
    f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['processed_quarantine_csv']}"
)
QUARANTINE_CSV_PATH = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['quarantine_csv']}"
REPORTS_DIR = f"{AIRFLOW_DATA_DIR}/{_CONFIG['paths']['reports_dir']}"

TARGET_DB = _CONFIG["database"]["target_db"]
CONN_ID = _CONFIG["database"]["conn_id"]
SODA_CONFIG_PATH = _CONFIG["soda"]["config_path"]
RAW_CONTRACT_PATH = _CONFIG["soda"]["raw_contract_path"]
PROCESSED_CONTRACT_PATH = _CONFIG["soda"]["processed_contract_path"]

RAW_CONTRACT_TABLE = _CONFIG["tables"]["raw_contract"]
PROCESSED_CONTRACT_TABLE = _CONFIG["tables"]["processed_contract"]
RAW_QUARANTINE_TABLE = _CONFIG["tables"]["raw_quarantine"]
PROCESSED_VALID_TABLE = _CONFIG["tables"]["processed_valid"]
PROCESSED_QUARANTINE_TABLE = _CONFIG["tables"]["processed_quarantine"]

PAGE_SIZE = _CONFIG["api"]["page_size"]
FIELDNAMES = _CONFIG["api"]["fieldnames"]
