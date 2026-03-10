"""
DAG 2 — Transformation
TaskGroup: transformation (parallèle)
  ├── transform_filter  → filtrage des données invalides
  └── transform_agg     → agrégation et restructuration
TaskGroup: quality
  └── validate_processed → Soda check post-transformation
"""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Constantes ────────────────────────────────────────────────────────────────
RAW_CSV_PATH       = "/usr/local/airflow/include/data/raw/chicago_crimes_raw.csv"
FILTERED_CSV_PATH  = "/usr/local/airflow/include/data/processed/chicago_crimes_filtered.csv"
AGGREGATED_CSV_PATH = "/usr/local/airflow/include/data/processed/chicago_crimes_aggregated.csv"
CLEAN_CSV_PATH     = "/usr/local/airflow/include/data/processed/chicago_crimes_clean.csv"


def transform_filter(**context):
    """
    Task 1a (parallèle) :
    - Lit le CSV brut
    - Supprime les lignes avec id, date, primary_type nuls
    - Supprime les doublons sur id
    - Cast des types (dates, booléens, numériques)
    - Sauvegarde le CSV filtré
    """
    print("⚙️ Transformation — filtrage...")
    df = pd.read_csv(RAW_CSV_PATH, dtype=str)
    initial_count = len(df)

    # Cast des types
    df["id"]           = pd.to_numeric(df["id"], errors="coerce")
    df["year"]         = pd.to_numeric(df["year"], errors="coerce")
    df["latitude"]     = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"]    = pd.to_numeric(df["longitude"], errors="coerce")
    df["x_coordinate"] = pd.to_numeric(df["x_coordinate"], errors="coerce")
    df["y_coordinate"] = pd.to_numeric(df["y_coordinate"], errors="coerce")
    df["arrest"]       = df["arrest"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["domestic"]     = df["domestic"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["date"]         = pd.to_datetime(df["date"], errors="coerce")
    df["updated_on"]   = pd.to_datetime(df["updated_on"], errors="coerce")

    # Normalisation colonnes
    df.columns = [col.lower().strip() for col in df.columns]

    # Filtrage : supprimer les lignes critiques nulles
    df.dropna(subset=["id", "date", "primary_type"], inplace=True)

    # Supprimer les doublons
    df.drop_duplicates(subset=["id"], inplace=True)

    os.makedirs(os.path.dirname(FILTERED_CSV_PATH), exist_ok=True)
    df.to_csv(FILTERED_CSV_PATH, index=False)

    print(f"✅ Filtrage OK : {initial_count} → {len(df)} lignes")
    print(f"   Lignes supprimées : {initial_count - len(df)}")


def transform_agg(**context):
    """
    Task 1b (parallèle) :
    - Lit le CSV brut
    - Agrège les données par type de crime et district
    - Sauvegarde le CSV agrégé (table de stats)
    """
    print("⚙️ Transformation — agrégation...")
    df = pd.read_csv(RAW_CSV_PATH, dtype=str)

    # Cast minimal pour l'agrégation
    df["arrest"]   = df["arrest"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["domestic"] = df["domestic"].map(
        {"true": True, "false": False, "True": True, "False": False}
    )
    df["year"]     = pd.to_numeric(df["year"], errors="coerce")

    # Agrégation par primary_type et district
    agg = df.groupby(["primary_type", "district"]).agg(
        total_crimes   = ("id", "count"),
        total_arrests  = ("arrest", "sum"),
        total_domestic = ("domestic", "sum"),
        year_min       = ("year", "min"),
        year_max       = ("year", "max"),
    ).reset_index()

    agg["arrest_rate"] = (agg["total_arrests"] / agg["total_crimes"] * 100).round(2)

    os.makedirs(os.path.dirname(AGGREGATED_CSV_PATH), exist_ok=True)
    agg.to_csv(AGGREGATED_CSV_PATH, index=False)

    print(f"✅ Agrégation OK : {len(agg)} groupes générés")


def merge_and_finalize(**context):
    """
    Task 2 :
    - Lit le CSV filtré (données nettoyées)
    - Sauvegarde le CSV final prêt pour le chargement
    """
    print("🔗 Finalisation du dataset propre...")
    df = pd.read_csv(FILTERED_CSV_PATH)

    os.makedirs(os.path.dirname(CLEAN_CSV_PATH), exist_ok=True)
    df.to_csv(CLEAN_CSV_PATH, index=False)
    print(f"✅ Dataset final prêt : {len(df)} lignes → {CLEAN_CSV_PATH}")


def validate_processed(**context):
    """
    Task 3 :
    - Exécute les checks Soda sur les données transformées
    - Stratégie FAIL HARD aussi (le DAG 3 ne se lance pas si données mauvaises)
    """
    from soda.scan import Scan

    print("🔍 Lancement des checks Soda sur les données transformées...")

    scan = Scan()
    scan.set_scan_definition_name("chicago_crimes_processed")
    scan.set_data_source_name("chicago_crimes_processed")
    scan.add_configuration_yaml_file(
        "/usr/local/airflow/include/soda/configuration.yml"
    )
    scan.add_sodacl_yaml_file(
        "/usr/local/airflow/include/soda/checks/transformed_checks.yml"
    )

    exit_code = scan.execute()
    logs = scan.get_logs_text()
    print(logs)

    if exit_code != 0:
        raise Exception(
            "❌ Soda check ÉCHOUÉ sur données transformées — pipeline arrêté\n"
            "Vérifiez les logs ci-dessus."
        )

    print("✅ Tous les checks Soda (processed) sont passés !")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_2_transformation",
    default_args=default_args,
    description="DAG 2 — Transformation parallèle + Soda check",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["transformation", "chicago", "soda"],
) as dag:

    # ── TaskGroup : Transformations en parallèle ──────────────────────────────
    with TaskGroup("transformation", tooltip="Transformations parallèles") as tg_transform:

        task_filter = PythonOperator(
            task_id="transform_filter",
            python_callable=transform_filter,
        )

        task_agg = PythonOperator(
            task_id="transform_agg",
            python_callable=transform_agg,
        )
    # ── Task : Merge et finalisation ─────────────────────────────────────────
    task_merge = PythonOperator(
        task_id="merge_and_finalize",
        python_callable=merge_and_finalize,
    )

    # ── TaskGroup : Validation qualité ───────────────────────────────────────
    with TaskGroup("quality", tooltip="Contrôle qualité post-transformation") as tg_quality:

        task_validate = PythonOperator(
            task_id="validate_processed",
            python_callable=validate_processed,
        )

    # ── Ordre d'exécution ─────────────────────────────────────────────────────
    # Ces deux tâches lisent le même CSV brut et peuvent s'exécuter en parallèle.
    [task_filter, task_agg] >> task_merge >> tg_quality
