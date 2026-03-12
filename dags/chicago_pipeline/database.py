import io

import pandas as pd
import psycopg2
from airflow.sdk import BaseHook

from dags.chicago_pipeline.constants import CONN_ID, TARGET_DB


def get_postgres_connection(database: str = TARGET_DB):
    conn_airflow = BaseHook.get_connection(CONN_ID)
    return psycopg2.connect(
        host=conn_airflow.host,
        port=conn_airflow.port,
        user=conn_airflow.login,
        password=conn_airflow.password,
        database=database,
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


def create_database_if_not_exists(**_context):
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
