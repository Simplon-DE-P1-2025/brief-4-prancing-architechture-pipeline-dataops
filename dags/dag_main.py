"""
DAG principal unifié du pipeline Chicago Crimes.

Le DAG expose directement trois TaskGroups visibles dans l'UI Airflow:
  1. ingestion
  2. transformation
  3. loading
"""
from datetime import datetime, timedelta
import csv
import io
import os
import yaml

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseHook, TaskGroup, Variable
from soda_core.contracts import verify_contract_locally

DAG_DOC_MD = """
# Chicago Crimes Pipeline

Pipeline ETL unique organise en trois groupes:

1. `ingestion`
   Extraction API, validation Soda du brut, publication des jeux `valid` et `quarantine`.
2. `transformation`
   Nettoyage, agregations et validation Soda du dataset prepare.
3. `loading`
   Initialisation PostgreSQL puis chargement parallelise des donnees valides et en quarantaine.

## Sorties locales

- `include/data/raw/`
- `include/data/processed/`
- `include/data/quarantine/`
- `include/data/reports/`

## Regles qualite

- Contrat raw: `include/soda/contracts/raw_contract.yml`
- Contrat processed: `include/soda/contracts/processed_contract.yml`
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AIRFLOW_DATA_DIR = "/usr/local/airflow/include/data"
RAW_EXTRACTED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/raw/chicago_crimes_raw_extracted.csv"
RAW_CSV_PATH = f"{AIRFLOW_DATA_DIR}/raw/chicago_crimes_raw.csv"
FILTERED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_filtered.csv"
AGGREGATED_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_aggregated.csv"
CLEAN_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_clean.csv"
RAW_QUARANTINE_CSV_PATH = f"{AIRFLOW_DATA_DIR}/quarantine/chicago_crimes_raw_quarantine.csv"
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


def get_postgres_connection():
    conn_airflow = BaseHook.get_connection(CONN_ID)
    return psycopg2.connect(
        host=conn_airflow.host,
        port=conn_airflow.port,
        user=conn_airflow.login,
        password=conn_airflow.password,
        database=TARGET_DB,
    )


def get_postgres_column_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "TEXT"


def replace_table_from_dataframe(df: pd.DataFrame, table_name: str):
    conn = get_postgres_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    columns_sql = ", ".join(
        f'"{column}" {get_postgres_column_type(df[column])}' for column in df.columns
    )
    cursor.execute(f'DROP TABLE IF EXISTS public."{table_name}"')
    cursor.execute(f'CREATE TABLE public."{table_name}" ({columns_sql})')

    if not df.empty:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)
        quoted_columns = ", ".join(f'"{column}"' for column in df.columns)
        cursor.copy_expert(
            f"""
            COPY public."{table_name}" ({quoted_columns})
            FROM STDIN WITH (FORMAT CSV)
            """,
            buffer,
        )

    conn.commit()
    cursor.close()
    conn.close()


def load_contract_definition(contract_path: str) -> dict:
    with open(contract_path, encoding="utf-8") as contract_file:
        return yaml.safe_load(contract_file)


def append_reason(reason_map: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    if not mask.any():
        return reason_map
    existing = reason_map.loc[mask].fillna("")
    separator = existing.ne("").map({True: "|", False: ""})
    reason_map.loc[mask] = existing + separator + reason
    return reason_map


def build_quarantine_from_contract(df: pd.DataFrame, contract_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    contract = load_contract_definition(contract_path)
    quarantine_mask = pd.Series(False, index=df.index)
    quarantine_reasons = pd.Series("", index=df.index, dtype="object")

    for column_definition in contract.get("columns", []):
        column_name = column_definition.get("name")
        if not column_name or column_name not in df.columns:
            continue

        column_series = df[column_name]
        checks = column_definition.get("checks", [])

        for check in checks:
            if not isinstance(check, dict) or len(check) != 1:
                continue

            check_name, check_config = next(iter(check.items()))

            if check_name == "missing":
                current_mask = column_series.isna() | (column_series.astype(str).str.strip() == "")
                quarantine_mask |= current_mask
                quarantine_reasons = append_reason(
                    quarantine_reasons, current_mask, f"{column_name}_missing"
                )

            elif check_name == "duplicate":
                non_missing_mask = ~(column_series.isna() | (column_series.astype(str).str.strip() == ""))
                current_mask = column_series.duplicated(keep=False) & non_missing_mask
                quarantine_mask |= current_mask
                quarantine_reasons = append_reason(
                    quarantine_reasons, current_mask, f"{column_name}_duplicate"
                )

            elif check_name == "invalid" and isinstance(check_config, dict):
                current_mask = pd.Series(False, index=df.index)

                valid_values = check_config.get("valid_values")
                if valid_values is not None:
                    non_missing_mask = ~(column_series.isna() | (column_series.astype(str).str.strip() == ""))
                    current_mask |= non_missing_mask & ~column_series.isin(valid_values)

                numeric_series = pd.to_numeric(column_series, errors="coerce")
                valid_min = check_config.get("valid_min")
                if valid_min is not None:
                    current_mask |= numeric_series.notna() & (numeric_series < valid_min)

                valid_max = check_config.get("valid_max")
                if valid_max is not None:
                    current_mask |= numeric_series.notna() & (numeric_series > valid_max)

                quarantine_mask |= current_mask
                quarantine_reasons = append_reason(
                    quarantine_reasons, current_mask, f"{column_name}_invalid"
                )

    df_valid = df[~quarantine_mask].copy()
    df_quarantine = df[quarantine_mask].copy()

    if not df_quarantine.empty:
        df_quarantine["quarantine_reason"] = quarantine_reasons.loc[df_quarantine.index]

    return df_valid, df_quarantine


def write_quality_reports(
    report_name: str,
    contract_path: str,
    total_rows: int,
    valid_rows: int,
    quarantine_rows: int,
    contract_passed: bool,
    df_quarantine: pd.DataFrame,
):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    try:
        os.chmod(REPORTS_DIR, 0o777)
    except PermissionError:
        print(f"Impossible de modifier les permissions de {REPORTS_DIR}")

    summary_rows = [
        {"report": report_name, "metric": "contract_path", "value": contract_path},
        {"report": report_name, "metric": "contract_passed", "value": contract_passed},
        {"report": report_name, "metric": "total_rows", "value": total_rows},
        {"report": report_name, "metric": "valid_rows", "value": valid_rows},
        {"report": report_name, "metric": "quarantine_rows", "value": quarantine_rows},
        {
            "report": report_name,
            "metric": "valid_ratio",
            "value": round((valid_rows / total_rows), 4) if total_rows else 0,
        },
    ]

    summary_df = pd.DataFrame(summary_rows)
    reasons_df = pd.DataFrame(columns=["quarantine_reason", "row_count"])
    if "quarantine_reason" in df_quarantine.columns and not df_quarantine.empty:
        reasons_df = (
            df_quarantine["quarantine_reason"]
            .fillna("")
            .str.split("|", regex=False)
            .explode()
            .loc[lambda series: series.ne("")]
            .value_counts()
            .rename_axis("quarantine_reason")
            .reset_index(name="row_count")
        )

    summary_csv_path = f"{REPORTS_DIR}/{report_name}_summary.csv"
    reasons_csv_path = f"{REPORTS_DIR}/{report_name}_reasons.csv"
    markdown_path = f"{REPORTS_DIR}/{report_name}_report.md"
    summary_df.to_csv(summary_csv_path, index=False)
    reasons_df.to_csv(reasons_csv_path, index=False)

    markdown_lines = [
        f"# {report_name}",
        "",
        f"- contract_path: `{contract_path}`",
        f"- contract_passed: `{contract_passed}`",
        f"- total_rows: `{total_rows}`",
        f"- valid_rows: `{valid_rows}`",
        f"- quarantine_rows: `{quarantine_rows}`",
        "",
        "## Top quarantine reasons",
        "",
    ]
    if reasons_df.empty:
        markdown_lines.append("No quarantine reasons.")
    else:
        markdown_lines.append("| quarantine_reason | row_count |")
        markdown_lines.append("|---|---:|")
        for row in reasons_df.itertuples(index=False):
            markdown_lines.append(f"| {row.quarantine_reason} | {row.row_count} |")

    with open(markdown_path, "w", encoding="utf-8") as markdown_file:
        markdown_file.write("\n".join(markdown_lines) + "\n")

    print(f"Rapports ecrits: {summary_csv_path}, {reasons_csv_path}, {markdown_path}")


def coerce_dataframe_for_contract(df: pd.DataFrame, contract_path: str) -> pd.DataFrame:
    contract = load_contract_definition(contract_path)
    coerced_df = df.copy()

    for column_definition in contract.get("columns", []):
        column_name = column_definition.get("name")
        if not column_name or column_name not in coerced_df.columns:
            continue

        checks = column_definition.get("checks", [])
        for check in checks:
            if not isinstance(check, dict) or len(check) != 1:
                continue

            check_name, check_config = next(iter(check.items()))
            if check_name == "invalid" and isinstance(check_config, dict):
                if "valid_min" in check_config or "valid_max" in check_config:
                    coerced_df[column_name] = pd.to_numeric(
                        coerced_df[column_name], errors="coerce"
                    )

    return coerced_df


def verify_contract(contract_path: str, fail_on_error: bool = True) -> bool:
    result = verify_contract_locally(
        data_source_file_path=SODA_CONFIG_PATH,
        contract_file_path=contract_path,
        publish=False,
    )

    get_logs = getattr(result, "get_logs", None)
    if callable(get_logs):
        for log_line in get_logs():
            print(log_line)

    if getattr(result, "has_errors", False):
        get_errors = getattr(result, "get_errors", None)
        if callable(get_errors):
            for error_line in get_errors():
                print(error_line)
        if fail_on_error:
            raise Exception(f"Contrat Soda en erreur: {contract_path}")
        return False

    if not getattr(result, "is_passed", False):
        if fail_on_error:
            raise Exception(f"Contrat Soda echoue: {contract_path}")
        return False

    return True


def fetch_and_save_csv(**context):
    http_conn = BaseHook.get_connection("chicago_crimes_api")
    base_url = f"{http_conn.schema}://{http_conn.host}"

    api_limit = Variable.get("CHICAGO_API_LIMIT", default="20000")
    endpoint = Variable.get("CHICAGO_API_ENDPOINT", default="/resource/ijzp-q8t2.json")

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

    os.makedirs(os.path.dirname(RAW_EXTRACTED_CSV_PATH), exist_ok=True)

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

    with open(RAW_EXTRACTED_CSV_PATH, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in data:
            writer.writerow(record)


def validate_raw(**context):
    df = pd.read_csv(RAW_EXTRACTED_CSV_PATH, dtype=str)
    df_for_contract = coerce_dataframe_for_contract(df, RAW_CONTRACT_PATH)
    replace_table_from_dataframe(df_for_contract, RAW_CONTRACT_TABLE)
    contract_passed = verify_contract(RAW_CONTRACT_PATH, fail_on_error=False)
    df_valid, df_quarantine = build_quarantine_from_contract(df, RAW_CONTRACT_PATH)
    write_quality_reports(
        report_name="raw_quality",
        contract_path=RAW_CONTRACT_PATH,
        total_rows=len(df),
        valid_rows=len(df_valid),
        quarantine_rows=len(df_quarantine),
        contract_passed=contract_passed,
        df_quarantine=df_quarantine,
    )
    print(
        "Validation raw terminee: "
        f"contrat_soda_ok={contract_passed}"
    )


def materialize_valid_raw(**context):
    df = pd.read_csv(RAW_EXTRACTED_CSV_PATH, dtype=str)
    df_valid, _ = build_quarantine_from_contract(df, RAW_CONTRACT_PATH)

    os.makedirs(os.path.dirname(RAW_CSV_PATH), exist_ok=True)
    df_valid.to_csv(RAW_CSV_PATH, index=False)
    print(f"Valid raw materialise: {len(df_valid)} lignes")


def materialize_quarantine_raw(**context):
    df = pd.read_csv(RAW_EXTRACTED_CSV_PATH, dtype=str)
    _, df_quarantine = build_quarantine_from_contract(df, RAW_CONTRACT_PATH)

    os.makedirs(os.path.dirname(RAW_QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(RAW_QUARANTINE_CSV_PATH, index=False)
    replace_table_from_dataframe(df_quarantine, RAW_QUARANTINE_TABLE)
    print(f"Quarantine raw materialisee: {len(df_quarantine)} lignes")


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
    df = pd.read_csv(CLEAN_CSV_PATH)
    df_for_contract = coerce_dataframe_for_contract(df, PROCESSED_CONTRACT_PATH)
    replace_table_from_dataframe(df_for_contract, PROCESSED_CONTRACT_TABLE)
    contract_passed = verify_contract(PROCESSED_CONTRACT_PATH, fail_on_error=False)
    df_valid, df_quarantine = build_quarantine_from_contract(df, PROCESSED_CONTRACT_PATH)
    write_quality_reports(
        report_name="processed_quality",
        contract_path=PROCESSED_CONTRACT_PATH,
        total_rows=len(df),
        valid_rows=len(df_valid),
        quarantine_rows=len(df_quarantine),
        contract_passed=contract_passed,
        df_quarantine=df_quarantine,
    )
    if not contract_passed:
        raise Exception(f"Contrat Soda echoue: {PROCESSED_CONTRACT_PATH}")


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
    df = pd.read_csv(CLEAN_CSV_PATH)
    mask_valid = (
        df["latitude"].between(41.6, 42.1, inclusive="both") | df["latitude"].isna()
    ) & (
        df["longitude"].between(-88.0, -87.5, inclusive="both")
        | df["longitude"].isna()
    )

    df_valid = df[mask_valid].copy()
    replace_table_from_dataframe(df_valid, "chicago_crimes")


def load_quarantine_data(**context):
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
    replace_table_from_dataframe(df_quarantine, "chicago_crimes_quarantine")


with DAG(
    dag_id="dag_main",
    default_args=default_args,
    description="Pipeline Chicago Crimes avec TaskGroups visibles",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    template_searchpath=["/usr/local/airflow/include"],
    doc_md=DAG_DOC_MD,
    tags=["main", "chicago", "pipeline"],
) as dag:
    with TaskGroup("ingestion", tooltip="Extraction et controle qualite brut") as ingestion:
        with TaskGroup("extract", tooltip="Extraction API et preparation de la base"):
            task_fetch_save = PythonOperator(
                task_id="fetch_api_raw",
                python_callable=fetch_and_save_csv,
                doc_md="Recupere les crimes depuis l'API Chicago et ecrit le CSV brut extrait.",
            )
            task_create_db = PythonOperator(
                task_id="ensure_postgres_database",
                python_callable=create_database_if_not_exists,
                doc_md="Cree la base PostgreSQL `chicago_crimes` si elle n'existe pas encore.",
            )

        with TaskGroup("quality", tooltip="Validation Soda du brut et publication des rapports"):
            task_validate_raw = PythonOperator(
                task_id="validate_raw_contract",
                python_callable=validate_raw,
                doc_md="Execute le contrat Soda raw sur le brut extrait et produit les rapports CSV et Markdown.",
            )

        with TaskGroup("outputs", tooltip="Separation du brut valide et de la quarantaine"):
            task_valid_raw = PythonOperator(
                task_id="publish_valid_raw",
                python_callable=materialize_valid_raw,
                doc_md="Materilise le sous-ensemble brut valide qui sera utilise par la transformation.",
            )
            task_quarantine_raw = PythonOperator(
                task_id="publish_quarantine_raw",
                python_callable=materialize_quarantine_raw,
                doc_md="Materilise les lignes brutes en echec dans un CSV et une table de quarantaine.",
            )

        task_fetch_save >> task_create_db >> task_validate_raw
        task_validate_raw >> [task_valid_raw, task_quarantine_raw]

    with TaskGroup("transformation", tooltip="Preparation et qualite des donnees") as transformation:
        with TaskGroup("prepare", tooltip="Nettoyage et enrichissement du dataset"):
            task_filter = PythonOperator(
                task_id="clean_valid_raw",
                python_callable=transform_filter,
                doc_md="Nettoie le brut valide, caste les types et supprime les doublons restants sur `id`.",
            )
            task_agg = PythonOperator(
                task_id="aggregate_valid_raw",
                python_callable=transform_agg,
                doc_md="Construit un dataset agrege par type de crime et district.",
            )
            task_merge = PythonOperator(
                task_id="finalize_clean_dataset",
                python_callable=merge_and_finalize,
                doc_md="Prepare le dataset final propre a partir du CSV filtre.",
            )

        with TaskGroup("quality", tooltip="Validation Soda du dataset prepare"):
            task_validate_processed = PythonOperator(
                task_id="validate_processed_contract",
                python_callable=validate_processed,
                doc_md="Execute le contrat Soda processed sur le dataset final avant chargement et produit les rapports CSV et Markdown.",
            )

        [task_filter, task_agg] >> task_merge >> task_validate_processed

    with TaskGroup("loading", tooltip="Initialisation base et chargement") as loading:
        with TaskGroup("init", tooltip="Preparation des tables PostgreSQL"):
            task_create_tables = SQLExecuteQueryOperator(
                task_id="create_target_tables",
                conn_id=CONN_ID,
                sql="sql/init_tables.sql",
                doc_md="Cree les tables PostgreSQL necessaires a partir du script SQL du projet.",
            )

        with TaskGroup("publish", tooltip="Chargement final parallelise"):
            task_load_valid = PythonOperator(
                task_id="load_valid_records",
                python_callable=load_valid_data,
                doc_md="Charge les lignes valides dans la table finale `chicago_crimes`.",
            )
            task_load_quarantine = PythonOperator(
                task_id="load_quarantine_records",
                python_callable=load_quarantine_data,
                doc_md="Charge les lignes hors bornes GPS dans la table de quarantaine finale.",
            )

        task_create_tables >> [task_load_valid, task_load_quarantine]

    task_valid_raw >> [task_filter, task_agg]
    transformation >> loading
