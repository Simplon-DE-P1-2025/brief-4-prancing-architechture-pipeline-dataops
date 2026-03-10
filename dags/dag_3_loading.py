"""
DAG 3 — Chargement PostgreSQL
TaskGroup: init_db
  ├── create_database_if_not_exists  → CREATE DATABASE chicago_crimes
  └── create_tables_if_not_exists   → CREATE TABLE IF NOT EXISTS
TaskGroup: loading (parallèle)
  ├── load_valid       → données valides → chicago_crimes
  └── load_quarantine  → données invalides → chicago_crimes_quarantine
"""
from datetime import datetime, timedelta
import pendulum
from airflow.sdk import DAG, BaseHook, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
import psycopg2
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Constantes ────────────────────────────────────────────────────────────────
CLEAN_CSV_PATH      = "/usr/local/airflow/include/data/processed/chicago_crimes_clean.csv"
QUARANTINE_CSV_PATH = "/usr/local/airflow/include/data/quarantine/chicago_crimes_quarantine.csv"
TARGET_DB           = "chicago_crimes"
CONN_ID             = "postgres_default"


def create_database_if_not_exists(**context):
    """
    Task 1a :
    - Se connecte à la base 'postgres' (base système toujours disponible)
    - Crée la base chicago_crimes si elle n'existe pas
    - Utilise autocommit=True car CREATE DATABASE ne supporte pas les transactions
    """
    # Connexion via Airflow (base système postgres)
    conn_airflow = BaseHook.get_connection("postgres_root")

    print(f"🔗 Connexion à PostgreSQL sur {conn_airflow.host}:{conn_airflow.port}")

    conn = psycopg2.connect(
        host=conn_airflow.host,
        port=conn_airflow.port,
        user=conn_airflow.login,
        password=conn_airflow.password,
        database="postgres",            # base système par défaut
    )
    conn.autocommit = True              # obligatoire pour CREATE DATABASE

    cursor = conn.cursor()

    # Vérifier si la base existe déjà
    cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
        (TARGET_DB,)
    )
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f'CREATE DATABASE "{TARGET_DB}"')
        print(f"✅ Base de données '{TARGET_DB}' créée")
    else:
        print(f"ℹ️  Base de données '{TARGET_DB}' existe déjà — rien à faire")

    cursor.close()
    conn.close()


def load_valid_data(**context):
    """
    Task 2a (parallèle) :
    - Lit le CSV propre
    - Identifie les lignes valides (latitude/longitude dans les bornes de Chicago)
    - Charge dans la table chicago_crimes via la Connection Airflow
    - Stratégie : REPLACE (données fraîches à chaque run)
    """
    from sqlalchemy import create_engine

    conn_airflow = BaseHook.get_connection(CONN_ID)
    engine = create_engine(
        f"postgresql://{conn_airflow.login}:{conn_airflow.password}"
        f"@{conn_airflow.host}:{conn_airflow.port}/{TARGET_DB}"
    )

    df = pd.read_csv(CLEAN_CSV_PATH)
    initial_count = len(df)

    # Définir les lignes valides
    mask_valid = (
        df["latitude"].between(41.6, 42.1, inclusive="both") |
        df["latitude"].isna()
    ) & (
        df["longitude"].between(-88.0, -87.5, inclusive="both") |
        df["longitude"].isna()
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

    print(f"✅ {len(df_valid)}/{initial_count} lignes valides chargées → table 'chicago_crimes'")


def load_quarantine_data(**context):
    """
    Task 2b (parallèle) :
    - Lit le CSV propre
    - Identifie les lignes invalides (coordonnées hors bornes Chicago)
    - Ajoute une colonne 'quarantine_reason' avec l'explication
    - Charge dans chicago_crimes_quarantine
    """
    from sqlalchemy import create_engine

    conn_airflow = BaseHook.get_connection(CONN_ID)
    engine = create_engine(
        f"postgresql://{conn_airflow.login}:{conn_airflow.password}"
        f"@{conn_airflow.host}:{conn_airflow.port}/{TARGET_DB}"
    )

    df = pd.read_csv(CLEAN_CSV_PATH)

    # Identifier les lignes invalides
    mask_invalid_lat = df["latitude"].notna() & ~df["latitude"].between(41.6, 42.1)
    mask_invalid_lon = df["longitude"].notna() & ~df["longitude"].between(-88.0, -87.5)
    mask_quarantine  = mask_invalid_lat | mask_invalid_lon

    df_quarantine = df[mask_quarantine].copy()

    if df_quarantine.empty:
        print("✅ Aucune ligne en quarantaine — toutes les données sont valides")
        return

    # Ajouter la raison de quarantaine
    df_quarantine["quarantine_reason"] = ""
    df_quarantine.loc[mask_invalid_lat & mask_quarantine, "quarantine_reason"] += "latitude_hors_bornes "
    df_quarantine.loc[mask_invalid_lon & mask_quarantine, "quarantine_reason"] += "longitude_hors_bornes"
    df_quarantine["quarantine_reason"] = df_quarantine["quarantine_reason"].str.strip()
    df_quarantine["quarantined_at"] = datetime.now()

    # Sauvegarder en CSV pour audit
    os.makedirs(os.path.dirname(QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(QUARANTINE_CSV_PATH, index=False)

    # Charger en base
    df_quarantine.to_sql(
        name="chicago_crimes_quarantine",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
        chunksize=1000,
    )

    print(f"⚠️  {len(df_quarantine)} lignes en quarantaine → table 'chicago_crimes_quarantine'")
    print(f"   CSV audit : {QUARANTINE_CSV_PATH}")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_3_loading",
    default_args=default_args,
    description="DAG 3 — Init DB/tables + Chargement PostgreSQL + Quarantaine",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["loading", "chicago", "postgres"],
) as dag:

    # ── TaskGroup : Initialisation DB et tables ───────────────────────────────
    with TaskGroup("init_db", tooltip="Création DB et tables si inexistantes") as tg_init:

        task_create_db = PythonOperator(
            task_id="create_database_if_not_exists",
            python_callable=create_database_if_not_exists,
        )

        task_create_tables = SQLExecuteQueryOperator(
            task_id="create_tables_if_not_exists",
            conn_id=CONN_ID,
            sql="/usr/local/airflow/include/sql/init_tables.sql",
        )

        # ── Ordre : créer la DB avant les tables ─────────────────────────────
        task_create_db >> task_create_tables

    # ── TaskGroup : Chargement en parallèle ──────────────────────────────────
    with TaskGroup("loading", tooltip="Chargement parallèle valid + quarantaine") as tg_loading:

        task_load_valid = PythonOperator(
            task_id="load_valid_data",
            python_callable=load_valid_data,
        )

        task_load_quarantine = PythonOperator(
            task_id="load_quarantine_data",
            python_callable=load_quarantine_data,
        )

        # Pas de dépendance entre elles → parallèle automatique
        [task_load_valid, task_load_quarantine]

    # ── Ordre d'exécution ─────────────────────────────────────────────────────
    tg_init >> tg_loading
