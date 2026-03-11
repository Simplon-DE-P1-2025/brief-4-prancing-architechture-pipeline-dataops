from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import sys

sys.path.insert(0, "/usr/local/airflow/include")
from pipeline_config import POSTGRES_CONN_ID


@dag(
    dag_id="chargement",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Déclenché par l'orchestrateur
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "chargement"],
)
def chargement():

    @task()
    def load_final_to_postgres():
        """Renomme chicago_crimes_clean en table de production finale."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS chicago_crimes_final;")
        cursor.execute("ALTER TABLE chicago_crimes_clean RENAME TO chicago_crimes_final;")

        conn.commit()
        cursor.close()
        conn.close()
        print("Chargement final OK : table chicago_crimes_final prête.")

    load_final_to_postgres()


chargement()
