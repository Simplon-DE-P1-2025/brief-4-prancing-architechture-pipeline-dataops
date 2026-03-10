"""
DAG principal unifié du pipeline Chicago Crimes.

Le DAG expose directement trois TaskGroups visibles dans l'UI Airflow:
  1. ingestion
  2. transformation
  3. loading
"""
from datetime import datetime, timedelta
import csv
import os

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseHook, TaskGroup, Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

RAW_CSV_PATH = "/usr/local/airflow/include/data/raw/chicago_crimes_raw.csv"
FILTERED_CSV_PATH = "/usr/local/airflow/include/data/processed/chicago_crimes_filtered.csv"
AGGREGATED_CSV_PATH = "/usr/local/airflow/include/data/processed/chicago_crimes_aggregated.csv"
CLEAN_CSV_PATH = "/usr/local/airflow/include/data/processed/chicago_crimes_clean.csv"
QUARANTINE_CSV_PATH = "/usr/local/airflow/include/data/quarantine/chicago_crimes_quarantine.csv"
TARGET_DB = "chicago_crimes"
CONN_ID = "postgres_default"


def fetch_and_save_csv(**context):
    http_conn = BaseHook.get_connection("chicago_crimes_api")
    base_url = f"{http_conn.schema}://{http_conn.host}"

    api_limit = Variable.get("CHICAGO_API_LIMIT", default_var="20000")
    endpoint = Variable.get(
        "CHICAGO_API_ENDPOINT", default_var="/resource/ijzp-q8t2.json"
    )

    url = f"{base_url}{endpoint}"
    params = {
        "$limit": api_limit,
        "$order": "date DESC",
    }

    print(f"Appel API : {url} avec limit={api_limit}")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    print(f"{len(data)} enregistrements recuperes")

    if not data:
        raise ValueError("L'API a retourne 0 enregistrement")

    os.makedirs(os.path.dirname(RAW_CSV_PATH), exist_ok=True)

    fieldnames = [
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

    with open(RAW_CSV_PATH, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in data:
            writer.writerow(record)


def validate_raw(**context):
    from soda.scan import Scan

    scan = Scan()
    scan.set_scan_definition_name("chicago_crimes_raw")
    scan.set_data_source_name("chicago_crimes_raw")
    scan.add_configuration_yaml_file("/usr/local/airflow/include/soda/configuration.yml")
    scan.add_sodacl_yaml_file("/usr/local/airflow/include/soda/checks/raw_checks.yml")

    exit_code = scan.execute()
    print(scan.get_logs_text())

    if exit_code != 0:
        raise Exception("Soda check raw echoue")


def transform_filter(**context):
    df = pd.read_csv(RAW_CSV_PATH, dtype=str)

    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["x_coordinate"] = pd.to_numeric(df["x_coordinate"], errors="coerce")
    df["y_coordinate"] = pd.to_numeric(df["y_coordinate"], errors="coerce")
    df["arrest"] = df["arrest"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["domestic"] = df["domestic"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["updated_on"] = pd.to_datetime(df["updated_on"], errors="coerce")
    df.columns = [col.lower().strip() for col in df.columns]

    df.dropna(subset=["id", "date", "primary_type"], inplace=True)
    df.drop_duplicates(subset=["id"], inplace=True)

    os.makedirs(os.path.dirname(FILTERED_CSV_PATH), exist_ok=True)
    df.to_csv(FILTERED_CSV_PATH, index=False)


def transform_agg(**context):
    df = pd.read_csv(RAW_CSV_PATH, dtype=str)

    df["arrest"] = df["arrest"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["domestic"] = df["domestic"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    agg = (
        df.groupby(["primary_type", "district"])
        .agg(
            total_crimes=("id", "count"),
            total_arrests=("arrest", "sum"),
            total_domestic=("domestic", "sum"),
            year_min=("year", "min"),
            year_max=("year", "max"),
        )
        .reset_index()
    )

    agg["arrest_rate"] = (agg["total_arrests"] / agg["total_crimes"] * 100).round(2)

    os.makedirs(os.path.dirname(AGGREGATED_CSV_PATH), exist_ok=True)
    agg.to_csv(AGGREGATED_CSV_PATH, index=False)


def merge_and_finalize(**context):
    df = pd.read_csv(FILTERED_CSV_PATH)
    os.makedirs(os.path.dirname(CLEAN_CSV_PATH), exist_ok=True)
    df.to_csv(CLEAN_CSV_PATH, index=False)


def validate_processed(**context):
    from soda.scan import Scan

    scan = Scan()
    scan.set_scan_definition_name("chicago_crimes_processed")
    scan.set_data_source_name("chicago_crimes_processed")
    scan.add_configuration_yaml_file("/usr/local/airflow/include/soda/configuration.yml")
    scan.add_sodacl_yaml_file(
        "/usr/local/airflow/include/soda/checks/transformed_checks.yml"
    )

    exit_code = scan.execute()
    print(scan.get_logs_text())

    if exit_code != 0:
        raise Exception("Soda check processed echoue")


def create_database_if_not_exists(**context):
    conn_airflow = BaseHook.get_connection("postgres_root")

    conn = psycopg2.connect(
        host=conn_airflow.host,
        port=conn_airflow.port,
        user=conn_airflow.login,
        password=conn_airflow.password,
        database="postgres",
    )
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
        (TARGET_DB,),
    )

    if not cursor.fetchone():
        cursor.execute(f'CREATE DATABASE "{TARGET_DB}"')

    cursor.close()
    conn.close()


def load_valid_data(**context):
    from sqlalchemy import create_engine

    conn_airflow = BaseHook.get_connection(CONN_ID)
    engine = create_engine(
        f"postgresql://{conn_airflow.login}:{conn_airflow.password}"
        f"@{conn_airflow.host}:{conn_airflow.port}/{TARGET_DB}"
    )

    df = pd.read_csv(CLEAN_CSV_PATH)
    mask_valid = (
        df["latitude"].between(41.6, 42.1, inclusive="both") | df["latitude"].isna()
    ) & (
        df["longitude"].between(-88.0, -87.5, inclusive="both")
        | df["longitude"].isna()
    )

    df_valid = df[mask_valid].copy()
    df_valid.to_sql(
        name="chicago_crimes",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
        chunksize=1000,
    )


def load_quarantine_data(**context):
    from sqlalchemy import create_engine

    conn_airflow = BaseHook.get_connection(CONN_ID)
    engine = create_engine(
        f"postgresql://{conn_airflow.login}:{conn_airflow.password}"
        f"@{conn_airflow.host}:{conn_airflow.port}/{TARGET_DB}"
    )

    df = pd.read_csv(CLEAN_CSV_PATH)
    mask_invalid_lat = df["latitude"].notna() & ~df["latitude"].between(41.6, 42.1)
    mask_invalid_lon = df["longitude"].notna() & ~df["longitude"].between(-88.0, -87.5)
    mask_quarantine = mask_invalid_lat | mask_invalid_lon

    df_quarantine = df[mask_quarantine].copy()
    if df_quarantine.empty:
        return

    df_quarantine["quarantine_reason"] = ""
    df_quarantine.loc[
        mask_invalid_lat & mask_quarantine, "quarantine_reason"
    ] += "latitude_hors_bornes "
    df_quarantine.loc[
        mask_invalid_lon & mask_quarantine, "quarantine_reason"
    ] += "longitude_hors_bornes"
    df_quarantine["quarantine_reason"] = df_quarantine["quarantine_reason"].str.strip()
    df_quarantine["quarantined_at"] = datetime.now()

    os.makedirs(os.path.dirname(QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(QUARANTINE_CSV_PATH, index=False)
    df_quarantine.to_sql(
        name="chicago_crimes_quarantine",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
        chunksize=1000,
    )


with DAG(
    dag_id="dag_main",
    default_args=default_args,
    description="Pipeline Chicago Crimes avec TaskGroups visibles",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["main", "chicago", "pipeline"],
) as dag:
    with TaskGroup("ingestion", tooltip="Extraction et controle qualite brut") as ingestion:
        task_fetch_save = PythonOperator(
            task_id="fetch_and_save_csv",
            python_callable=fetch_and_save_csv,
        )
        task_validate_raw = PythonOperator(
            task_id="validate_raw",
            python_callable=validate_raw,
        )
        task_fetch_save >> task_validate_raw

    with TaskGroup("transformation", tooltip="Preparation et qualite des donnees") as transformation:
        task_filter = PythonOperator(
            task_id="transform_filter",
            python_callable=transform_filter,
        )
        task_agg = PythonOperator(
            task_id="transform_agg",
            python_callable=transform_agg,
        )
        task_merge = PythonOperator(
            task_id="merge_and_finalize",
            python_callable=merge_and_finalize,
        )
        task_validate_processed = PythonOperator(
            task_id="validate_processed",
            python_callable=validate_processed,
        )
        [task_filter, task_agg] >> task_merge >> task_validate_processed

    with TaskGroup("loading", tooltip="Initialisation base et chargement") as loading:
        task_create_db = PythonOperator(
            task_id="create_database_if_not_exists",
            python_callable=create_database_if_not_exists,
        )
        task_create_tables = SQLExecuteQueryOperator(
            task_id="create_tables_if_not_exists",
            conn_id=CONN_ID,
            sql="/usr/local/airflow/include/sql/init_tables.sql",
        )
        task_load_valid = PythonOperator(
            task_id="load_valid_data",
            python_callable=load_valid_data,
        )
        task_load_quarantine = PythonOperator(
            task_id="load_quarantine_data",
            python_callable=load_quarantine_data,
        )
        task_create_db >> task_create_tables >> [task_load_valid, task_load_quarantine]

    ingestion >> transformation >> loading
