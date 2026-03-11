from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
import os
from psycopg2.extras import execute_values
from soda.scan import Scan


API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_BATCH_SIZE = 20000
MAX_ROWS = 100000  # Mettre None pour récupérer tout le jeu de données (~8M lignes)
RAW_CSV = "/usr/local/airflow/include/data/chicago_crimes_raw.csv"
CLEAN_CSV = "/usr/local/airflow/include/data/chicago_crimes_clean.csv"
SODA_CONFIG = "/usr/local/airflow/include/soda/soda_config.yml"
CHECKS_RAW = "/usr/local/airflow/include/soda/checks_raw.yml"
CHECKS_CLEAN = "/usr/local/airflow/include/soda/checks_clean.yml"

RAW_COLS = ["id", "date", "primary_type", "description",
            "location_description", "arrest", "domestic",
            "district", "ward", "community_area", "year",
            "latitude", "longitude"]


def run_soda_check(checks_file: str, dataset: str) -> None:
    """Lance un scan Soda et lève une exception si des checks échouent."""
    scan = Scan()
    scan.set_scan_definition_name(dataset)
    scan.set_data_source_name("chicago_crimes_db")
    scan.add_configuration_yaml_file(file_path=SODA_CONFIG)
    scan.add_sodacl_yaml_file(file_path=checks_file)
    scan.execute()

    if scan.has_check_fails():
        raise ValueError(f"Soda checks failed for {dataset}:\n{scan.get_logs_text()}")


@dag(
    dag_id="chicago_crimes_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "chicago", "soda"],
    doc_md="""
    ## Pipeline Chicago Crimes
    Pipeline DataOps complet :
    1. Ingestion depuis l'API Chicago crimes (pagination par lots)
    2. Contrôle qualité Soda sur données brutes
    3. Transformation (nettoyage, filtrage, typage)
    4. Contrôle qualité Soda sur données transformées
    5. Chargement final en PostgreSQL

    **MAX_ROWS** : mettre `None` pour tout récupérer (~8M lignes).
    """,
)
def chicago_crimes_pipeline():

    @task()
    def ingest_data():
        """Étape 1 : Récupère toutes les données en parallèle et écrit par lot (économie mémoire)."""
        os.makedirs(os.path.dirname(RAW_CSV), exist_ok=True)

        # Récupérer le nombre total de lignes disponibles via l'API
        count_resp = requests.get(
            API_URL, params={"$select": "count(*)", "$limit": 1}, timeout=30
        )
        count_resp.raise_for_status()
        total_available = int(count_resp.json()[0]["count"])
        total_to_fetch = min(total_available, MAX_ROWS) if MAX_ROWS else total_available
        offsets = list(range(0, total_to_fetch, API_BATCH_SIZE))
        print(f"Total disponible : {total_available} | À récupérer : {total_to_fetch} | Lots : {len(offsets)}")

        def fetch_batch(offset):
            """Récupère un lot depuis l'API avec retry en cas de timeout."""
            limit = min(API_BATCH_SIZE, total_to_fetch - offset)
            params = {"$limit": limit, "$offset": offset}
            for attempt in range(1, 4):
                try:
                    resp = requests.get(API_URL, params=params, timeout=120)
                    resp.raise_for_status()
                    batch = resp.json()
                    print(f"  Lot récupéré : {len(batch)} lignes (offset={offset})")
                    return offset, batch
                except requests.exceptions.Timeout:
                    print(f"  Timeout offset={offset} tentative {attempt}/3, retry...")
                    if attempt == 3:
                        raise
            return offset, []

        # Fetch en parallèle (5 requêtes simultanées)
        WINDOW = 5
        total_written = 0
        first = True

        for i in range(0, len(offsets), WINDOW):
            window_offsets = offsets[i:i + WINDOW]

            with ThreadPoolExecutor(max_workers=WINDOW) as executor:
                futures = {executor.submit(fetch_batch, off): off for off in window_offsets}
                window_results = {}
                for future in as_completed(futures):
                    offset, batch = future.result()
                    window_results[offset] = batch

            for offset in sorted(window_results.keys()):
                batch = window_results[offset]
                if not batch:
                    continue
                df = pd.DataFrame(batch)
                df.to_csv(RAW_CSV, mode="w" if first else "a", header=first, index=False)
                total_written += len(df)
                first = False
                del df

            del window_results
            print(f"Fenêtre {i // WINDOW + 1} traitée — total écrit : {total_written:,}")

        print(f"Ingestion terminée : {total_written} lignes dans {RAW_CSV}")
        return total_written

    @task()
    def load_raw_to_postgres(row_count: int):
        """Charge les données brutes en base via bulk insert pour les checks Soda."""
        df = pd.read_csv(RAW_CSV)

        # Garder uniquement les colonnes connues présentes dans le CSV
        cols = [c for c in RAW_COLS if c in df.columns]
        df = df[cols]

        # Remplacer NaN par None pour psycopg2
        df = df.where(pd.notnull(df), None)

        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS chicago_crimes_raw CASCADE;")
        cursor.execute(f"""
            CREATE TABLE chicago_crimes_raw (
                {', '.join(f'{c} TEXT' for c in cols)}
            );
        """)

        records = list(df.itertuples(index=False, name=None))
        execute_values(
            cursor,
            f"INSERT INTO chicago_crimes_raw ({', '.join(cols)}) VALUES %s",
            records,
            page_size=1000
        )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Chargement brut OK : {len(df)} lignes dans chicago_crimes_raw")

    @task()
    def soda_check_raw():
        """Étape 2 : Contrôle qualité Soda sur les données brutes."""
        run_soda_check(CHECKS_RAW, "chicago_crimes_raw")
        print("Soda check RAW : OK")

    @task()
    def transform_data():
        """Étape 3 : Nettoyage et transformation des données."""
        df = pd.read_csv(RAW_CSV)

        cols = ["id", "date", "primary_type", "description",
                "arrest", "domestic", "district", "year",
                "latitude", "longitude"]
        df = df[[c for c in cols if c in df.columns]].copy()

        df.rename(columns={"primary_type": "crime_type"}, inplace=True)
        df.dropna(subset=["id", "crime_type", "date"], inplace=True)
        df.drop_duplicates(subset=["id"], inplace=True)

        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["arrest"] = df["arrest"].map({"true": True, "false": False,
                                          "True": True, "False": False})
        df = df[(df["year"] >= 2000) & (df["year"] <= 2025)]
        df["crime_type"] = df["crime_type"].str.strip().str.upper()

        df.to_csv(CLEAN_CSV, index=False)
        print(f"Transformation OK : {len(df)} lignes dans {CLEAN_CSV}")
        return len(df)

    @task()
    def load_clean_to_postgres(row_count: int):
        """Charge les données transformées en base via bulk insert."""
        df = pd.read_csv(CLEAN_CSV)

        # Convertir arrest en bool Python ou None (NaN/string -> None)
        def to_bool(val):
            if pd.isna(val) or val is None:
                return None
            if str(val).strip().lower() in ("true", "1"):
                return True
            if str(val).strip().lower() in ("false", "0"):
                return False
            return None

        if "arrest" in df.columns:
            df["arrest"] = df["arrest"].apply(to_bool)

        df = df.where(pd.notnull(df), None)

        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS chicago_crimes_clean CASCADE;")
        cursor.execute("""
            CREATE TABLE chicago_crimes_clean (
                id TEXT,
                date TEXT,
                crime_type TEXT,
                description TEXT,
                arrest BOOLEAN,
                domestic TEXT,
                district TEXT,
                year INTEGER,
                latitude FLOAT,
                longitude FLOAT
            );
        """)

        clean_cols = ["id", "date", "crime_type", "description",
                      "arrest", "domestic", "district", "year",
                      "latitude", "longitude"]
        present_cols = [c for c in clean_cols if c in df.columns]
        records = list(df[present_cols].itertuples(index=False, name=None))

        execute_values(
            cursor,
            f"INSERT INTO chicago_crimes_clean ({', '.join(present_cols)}) VALUES %s",
            records,
            page_size=1000
        )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Chargement clean OK : {len(df)} lignes dans chicago_crimes_clean")

    @task()
    def soda_check_clean():
        """Étape 4 : Contrôle qualité Soda sur les données transformées."""
        run_soda_check(CHECKS_CLEAN, "chicago_crimes_clean")
        print("Soda check CLEAN : OK")

    @task()
    def load_final_to_postgres():
        """Étape 5 : Renomme chicago_crimes_clean en table de production."""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS chicago_crimes_final;")
        cursor.execute("ALTER TABLE chicago_crimes_clean RENAME TO chicago_crimes_final;")

        conn.commit()
        cursor.close()
        conn.close()
        print("Chargement final OK : table chicago_crimes_final prête.")

    # Orchestration
    raw_count = ingest_data()
    load_raw = load_raw_to_postgres(raw_count)
    check_raw = soda_check_raw()
    clean_count = transform_data()
    load_clean = load_clean_to_postgres(clean_count)
    check_clean = soda_check_clean()
    final = load_final_to_postgres()

    raw_count >> load_raw >> check_raw >> clean_count >> load_clean >> check_clean >> final


chicago_crimes_pipeline()
