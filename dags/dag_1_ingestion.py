"""
DAG 1 — Ingestion
TaskGroup: ingestion
  ├── task_1 : fetch_and_save_csv  → appel API via HTTP Connection + save CSV
  └── task_2 : validate_raw        → Soda check (fail hard si erreur)
"""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, BaseHook, TaskGroup, Variable
from airflow.providers.standard.operators.python import PythonOperator
import requests
import csv
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Constantes ────────────────────────────────────────────────────────────────
RAW_CSV_PATH = "/usr/local/airflow/include/data/raw/chicago_crimes_raw.csv"


def fetch_and_save_csv(**context):
    """
    Task 1 :
    - Récupère les données depuis l'API Chicago Crimes via la HTTP Connection Airflow
    - Sauvegarde chaque enregistrement JSON en ligne CSV
    """
    # Récupérer la connexion HTTP depuis Airflow
    http_conn = BaseHook.get_connection("chicago_crimes_api")
    base_url  = f"{http_conn.schema}://{http_conn.host}"

    # Récupérer les variables Airflow
    api_limit  = Variable.get("CHICAGO_API_LIMIT", default_var="20000")
    endpoint   = Variable.get("CHICAGO_API_ENDPOINT", default_var="/resource/ijzp-q8t2.json")

    url = f"{base_url}{endpoint}"
    params = {
        "$limit": api_limit,
        "$order": "date DESC",
    }

    # Appel API
    print(f"🔗 Appel API : {url} avec limit={api_limit}")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    print(f"✅ {len(data)} enregistrements récupérés")

    if not data:
        raise ValueError("❌ L'API a retourné 0 enregistrement — pipeline arrêté")

    # Sauvegarde en CSV
    os.makedirs(os.path.dirname(RAW_CSV_PATH), exist_ok=True)

    fieldnames = [
        "id", "case_number", "date", "block", "iucr",
        "primary_type", "description", "location_description",
        "arrest", "domestic", "beat", "district", "ward",
        "community_area", "fbi_code", "x_coordinate", "y_coordinate",
        "year", "updated_on", "latitude", "longitude",
    ]

    with open(RAW_CSV_PATH, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in data:
            writer.writerow(record)

    print(f"💾 CSV sauvegardé : {RAW_CSV_PATH} ({len(data)} lignes)")


def validate_raw(**context):
    """
    Task 2 :
    - Exécute les checks Soda sur le CSV brut
    - Stratégie FAIL HARD : si un check échoue, le DAG s'arrête
    """
    from soda.scan import Scan

    print("🔍 Lancement des checks Soda sur les données brutes...")

    scan = Scan()
    scan.set_scan_definition_name("chicago_crimes_raw")
    scan.set_data_source_name("chicago_crimes_raw")
    scan.add_configuration_yaml_file(
        "/usr/local/airflow/include/soda/configuration.yml"
    )
    scan.add_sodacl_yaml_file(
        "/usr/local/airflow/include/soda/checks/raw_checks.yml"
    )

    exit_code = scan.execute()
    logs = scan.get_logs_text()
    print(logs)

    if exit_code != 0:
        raise Exception(
            "❌ Soda check ÉCHOUÉ sur données brutes — pipeline arrêté (fail hard)\n"
            "Vérifiez les logs ci-dessus pour identifier les problèmes."
        )

    print("✅ Tous les checks Soda (raw) sont passés !")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_1_ingestion",
    default_args=default_args,
    description="DAG 1 — Ingestion API + Save CSV + Soda check brut",
    schedule=None,                      # déclenché par dag_main uniquement
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ingestion", "chicago", "soda"],
) as dag:

    with TaskGroup("ingestion", tooltip="Extraction et validation brute") as tg_ingestion:
        with TaskGroup("extract", tooltip="Récupération et sauvegarde des données brutes"):
            task_fetch_save = PythonOperator(
                task_id="fetch_and_save_csv",
                python_callable=fetch_and_save_csv,
            )

        with TaskGroup("quality", tooltip="Contrôle qualité sur le CSV brut"):
            task_validate_raw = PythonOperator(
                task_id="validate_raw",
                python_callable=validate_raw,
            )

        # L'étape qualité dépend de l'extraction, il n'y a pas de parallélisme utile ici.
        task_fetch_save >> task_validate_raw
