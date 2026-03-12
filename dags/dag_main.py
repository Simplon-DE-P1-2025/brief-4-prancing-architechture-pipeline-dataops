"""
DAG principal unifie du pipeline Chicago Crimes avec pagination API.

Le DAG expose directement trois TaskGroups visibles dans l'UI Airflow:
  1. ingestion
  2. transformation
  3. loading
"""

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from dags.chicago_pipeline.constants import CONN_ID, DAG_DOC_MD, DEFAULT_ARGS
from dags.chicago_pipeline.database import create_database_if_not_exists
from dags.chicago_pipeline.extraction import fetch_and_save_csv
from dags.chicago_pipeline.loading import (
    load_agg_community,
    load_agg_hourly,
    load_agg_monthly,
    load_agg_serious,
    load_agg_yearly,
    load_quarantine_data,
    load_valid_data,
)
from dags.chicago_pipeline.quality import (
    materialize_quarantine_processed,
    materialize_quarantine_raw,
    materialize_valid_processed,
    materialize_valid_raw,
    split_processed_outputs,
    validate_processed,
    validate_raw,
)
from dags.chicago_pipeline.transformation import (
    merge_and_finalize,
    transform_agg,
    transform_agg_community,
    transform_agg_hourly,
    transform_agg_monthly,
    transform_agg_serious,
    transform_agg_yearly,
    transform_filter,
)


with DAG(
    dag_id="dag_main",
    default_args=DEFAULT_ARGS,
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
        with TaskGroup("prepare", tooltip="Nettoyage et preparation du dataset principal"):
            task_filter = PythonOperator(
                task_id="clean_valid_raw",
                python_callable=transform_filter,
                doc_md="Nettoie le brut valide, caste les types et supprime les doublons.",
            )
            task_merge = PythonOperator(
                task_id="finalize_clean_dataset",
                python_callable=merge_and_finalize,
                doc_md="Prepare le dataset final propre a partir du CSV filtre.",
            )

        with TaskGroup("aggregations", tooltip="Construction des jeux agreges") as aggregations:
            task_agg = PythonOperator(
                task_id="aggregate_valid_raw",
                python_callable=transform_agg,
                doc_md="Construit un dataset agrege par type de crime et district.",
            )
            task_agg_hourly = PythonOperator(
                task_id="aggregate_hourly",
                python_callable=transform_agg_hourly,
                doc_md="Agregation par tranche horaire (nuit, matin, apres-midi, soir).",
            )
            task_agg_monthly = PythonOperator(
                task_id="aggregate_monthly",
                python_callable=transform_agg_monthly,
                doc_md="Agregation mensuelle pour analyser la saisonnalite.",
            )
            task_agg_serious = PythonOperator(
                task_id="aggregate_serious_crimes",
                python_callable=transform_agg_serious,
                doc_md="Agregation des crimes graves (homicide, assault, robbery...).",
            )
            task_agg_community = PythonOperator(
                task_id="aggregate_community",
                python_callable=transform_agg_community,
                doc_md="Agregation par quartier avec taux d'arrestation et violence domestique.",
            )
            task_agg_yearly = PythonOperator(
                task_id="aggregate_yearly",
                python_callable=transform_agg_yearly,
                doc_md="Tendance annuelle du nombre de crimes par type.",
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

        task_filter >> task_merge >> task_validate_processed
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

        with TaskGroup("records", tooltip="Chargement des donnees finales valides et en quarantaine"):
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

        with TaskGroup("aggregations", tooltip="Chargement des tables d agregation"):
            task_load_hourly = PythonOperator(
                task_id="load_agg_hourly",
                python_callable=load_agg_hourly,
                doc_md="Charge l'agregation par tranche horaire.",
            )
            task_load_monthly = PythonOperator(
                task_id="load_agg_monthly",
                python_callable=load_agg_monthly,
                doc_md="Charge l'agregation mensuelle.",
            )
            task_load_serious = PythonOperator(
                task_id="load_agg_serious",
                python_callable=load_agg_serious,
                doc_md="Charge l'agregation des crimes graves.",
            )
            task_load_community = PythonOperator(
                task_id="load_agg_community",
                python_callable=load_agg_community,
                doc_md="Charge l'agregation par quartier.",
            )
            task_load_yearly = PythonOperator(
                task_id="load_agg_yearly",
                python_callable=load_agg_yearly,
                doc_md="Charge l'agregation annuelle.",
            )

        task_create_tables >> [task_load_valid, task_load_quarantine]
        task_create_tables >> [
            task_load_hourly,
            task_load_monthly,
            task_load_serious,
            task_load_community,
            task_load_yearly,
        ]

    task_valid_raw >> task_filter
    task_valid_raw >> [
        task_agg,
        task_agg_hourly,
        task_agg_monthly,
        task_agg_serious,
        task_agg_community,
        task_agg_yearly,
    ]
    [task_valid_processed, task_quarantine_processed] >> task_create_tables
