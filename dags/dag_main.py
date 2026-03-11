"""
DAG principal unifie du pipeline Chicago Crimes avec pagination API.

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
PROCESSED_VALID_CSV_PATH = f"{AIRFLOW_DATA_DIR}/processed/chicago_crimes_valid.csv"
RAW_QUARANTINE_CSV_PATH = f"{AIRFLOW_DATA_DIR}/quarantine/chicago_crimes_raw_quarantine.csv"
PROCESSED_QUARANTINE_CSV_PATH = f"{AIRFLOW_DATA_DIR}/quarantine/chicago_crimes_processed_quarantine.csv"
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
    """
    Recupere les donnees de l'API Chicago Crimes avec pagination Socrata
    et ecrit les pages directement dans le CSV local.
    """
    http_conn = BaseHook.get_connection("chicago_crimes_api")
    base_url = f"{http_conn.schema}://{http_conn.host}"
    api_limit = int(Variable.get("CHICAGO_API_LIMIT", default="20000"))
    endpoint = Variable.get("CHICAGO_API_ENDPOINT", default="/resource/ijzp-q8t2.json")
    url = f"{base_url}{endpoint}"

    offset = 0
    page = 1
    total_rows = 0

    print(f"Debut pagination - URL: {url}")
    print(f"PAGE_SIZE={PAGE_SIZE} | LIMIT total={api_limit}")

    os.makedirs(os.path.dirname(RAW_EXTRACTED_CSV_PATH), exist_ok=True)
    with open(RAW_EXTRACTED_CSV_PATH, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()

        with requests.Session() as session:
            while offset < api_limit:
                current_limit = min(PAGE_SIZE, api_limit - offset)
                params = {
                    "$limit": current_limit,
                    "$offset": offset,
                    "$order": "id ASC",
                }

                print(f"Page {page} - offset={offset} | limit={current_limit}")
                response = session.get(url, params=params, timeout=60)
                response.raise_for_status()
                page_data = response.json()

                if not page_data:
                    print(f"Fin pagination - page vide a l'offset {offset}")
                    break

                for record in page_data:
                    writer.writerow(record)

                batch_size = len(page_data)
                total_rows += batch_size
                print(f"  {batch_size} enregistrements recus | Total cumule: {total_rows}")

                if batch_size < current_limit:
                    print("Fin pagination - derniere page atteinte")
                    break

                offset += batch_size
                page += 1

    if total_rows == 0:
        raise ValueError("L'API a retourne 0 enregistrement - pipeline arrete")

    print(f"Pagination terminee - {total_rows} enregistrements ecrits dans {RAW_EXTRACTED_CSV_PATH}")


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
    print(f"Validation raw terminee: contrat_soda_ok={contract_passed}")


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
    print(f"Validation processed terminee: contrat_soda_ok={contract_passed}")


def split_processed_outputs(**context):
    df = pd.read_csv(CLEAN_CSV_PATH)
    df_valid, df_quarantine = build_quarantine_from_contract(df, PROCESSED_CONTRACT_PATH)

    os.makedirs(os.path.dirname(PROCESSED_VALID_CSV_PATH), exist_ok=True)
    df_valid.to_csv(PROCESSED_VALID_CSV_PATH, index=False)

    if not df_quarantine.empty:
        df_quarantine["quarantined_at"] = datetime.now()

    os.makedirs(os.path.dirname(PROCESSED_QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(PROCESSED_QUARANTINE_CSV_PATH, index=False)
    print(
        "Split processed termine: "
        f"{len(df_valid)} lignes valides, {len(df_quarantine)} lignes en quarantaine"
    )


def materialize_valid_processed(**context):
    df_valid = pd.read_csv(PROCESSED_VALID_CSV_PATH)
    replace_table_from_dataframe(df_valid, PROCESSED_VALID_TABLE)
    print(f"Processed valid materialise: {len(df_valid)} lignes")


def materialize_quarantine_processed(**context):
    df_quarantine = pd.read_csv(PROCESSED_QUARANTINE_CSV_PATH)
    replace_table_from_dataframe(df_quarantine, PROCESSED_QUARANTINE_TABLE)
    print(f"Processed quarantine materialisee: {len(df_quarantine)} lignes")


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
    df_valid = pd.read_csv(PROCESSED_VALID_CSV_PATH)
    replace_table_from_dataframe(df_valid, "chicago_crimes")


def load_quarantine_data(**context):
    df_quarantine = pd.read_csv(PROCESSED_QUARANTINE_CSV_PATH)
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
                doc_md="Recupere les crimes depuis l'API Chicago avec pagination et ecrit le CSV brut extrait.",
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
                doc_md="Materialise le sous-ensemble brut valide.",
            )
            task_quarantine_raw = PythonOperator(
                task_id="publish_quarantine_raw",
                python_callable=materialize_quarantine_raw,
                doc_md="Materialise les lignes brutes en echec dans un CSV et une table de quarantaine.",
            )

        task_fetch_save >> task_create_db >> task_validate_raw
        task_validate_raw >> [task_valid_raw, task_quarantine_raw]

    with TaskGroup("transformation", tooltip="Preparation et qualite des donnees") as transformation:
        with TaskGroup("prepare", tooltip="Nettoyage et enrichissement du dataset"):
            task_filter = PythonOperator(
                task_id="clean_valid_raw",
                python_callable=transform_filter,
                doc_md="Nettoie le brut valide, caste les types et supprime les doublons.",
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
                doc_md="Execute le contrat Soda processed sur le dataset final et produit les rapports de qualite. La suite du flux se base ensuite sur le split valid/quarantine.",
            )

        with TaskGroup("outputs", tooltip="Separation du dataset processed valide et de la quarantaine"):
            task_split_processed = PythonOperator(
                task_id="split_processed_outputs",
                python_callable=split_processed_outputs,
                doc_md="Construit une seule fois les jeux processed valides et en quarantaine a partir des regles du contrat Soda.",
            )
            task_valid_processed = PythonOperator(
                task_id="publish_valid_processed",
                python_callable=materialize_valid_processed,
                doc_md="Publie le jeu processed valide dans la table intermediaire `chicago_crimes_processed_valid`.",
            )
            task_quarantine_processed = PythonOperator(
                task_id="publish_quarantine_processed",
                python_callable=materialize_quarantine_processed,
                doc_md="Publie le jeu processed en quarantaine dans la table intermediaire `chicago_crimes_processed_quarantine`.",
            )

        [task_filter, task_agg] >> task_merge >> task_validate_processed
        task_validate_processed >> task_split_processed
        task_split_processed >> [task_valid_processed, task_quarantine_processed]

    with TaskGroup("loading", tooltip="Preparation SQL et chargement final") as loading:
        with TaskGroup("init", tooltip="Preparation des tables cibles PostgreSQL"):
            task_create_tables = SQLExecuteQueryOperator(
                task_id="create_target_tables",
                conn_id=CONN_ID,
                sql="sql/init_tables.sql",
                doc_md="Cree ou reinitialise les tables cibles PostgreSQL a partir du script SQL du projet.",
            )

        with TaskGroup("publish", tooltip="Chargement final parallelise"):
            task_load_valid = PythonOperator(
                task_id="load_valid_records",
                python_callable=load_valid_data,
                doc_md="Charge les lignes de `processed_valid` dans la table finale `chicago_crimes`.",
            )
            task_load_quarantine = PythonOperator(
                task_id="load_quarantine_records",
                python_callable=load_quarantine_data,
                doc_md="Charge les lignes de `processed_quarantine` dans la table finale de quarantaine.",
            )

        task_create_tables >> [task_load_valid, task_load_quarantine]

    task_valid_raw >> [task_filter, task_agg]
    [task_valid_processed, task_quarantine_processed] >> task_create_tables
