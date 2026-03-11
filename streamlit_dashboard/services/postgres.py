from typing import Any

import pandas as pd
import psycopg2
from psycopg2 import OperationalError
import streamlit as st

from streamlit_dashboard.config import load_soda_connection_config


def get_db_config() -> dict[str, Any]:
    return load_soda_connection_config()


def _connect():
    return psycopg2.connect(**get_db_config())


@st.cache_data(show_spinner=False, ttl=30)
def get_db_connection_error() -> str | None:
    try:
        conn = _connect()
        conn.close()
        return None
    except OperationalError as exc:
        return str(exc).strip()


def _query_dataframe(query: str, params: list[Any] | None = None) -> pd.DataFrame:
    try:
        with _connect() as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=30)
def get_public_tables() -> pd.DataFrame:
    return _query_dataframe(
        """
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
        """
    )


@st.cache_data(show_spinner=False, ttl=30)
def get_table_row_counts() -> pd.DataFrame:
    tables_df = get_public_tables()
    if tables_df.empty:
        return pd.DataFrame(columns=["table_name", "row_count"])

    counts: list[dict[str, Any]] = []
    try:
        with _connect() as conn:
            with conn.cursor() as cursor:
                for table_name in tables_df["tablename"].tolist():
                    cursor.execute(f'SELECT COUNT(*) FROM public."{table_name}"')
                    counts.append(
                        {
                            "table_name": table_name,
                            "row_count": cursor.fetchone()[0],
                        }
                    )
    except Exception:
        return pd.DataFrame(columns=["table_name", "row_count"])

    return pd.DataFrame(counts).sort_values("row_count", ascending=False)


@st.cache_data(show_spinner=False, ttl=30)
def get_table_columns(table_name: str) -> pd.DataFrame:
    return _query_dataframe(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        [table_name],
    )


@st.cache_data(show_spinner=False, ttl=30)
def get_table_preview(table_name: str, limit: int = 100) -> pd.DataFrame:
    return _query_dataframe(
        f'SELECT * FROM public."{table_name}" LIMIT {int(limit)}'
    )


@st.cache_data(show_spinner=False, ttl=30)
def get_quarantine_tables() -> pd.DataFrame:
    tables_df = get_public_tables()
    if tables_df.empty:
        return pd.DataFrame()
    return tables_df.loc[
        tables_df["tablename"].str.contains("quarantine", case=False, na=False)
    ].reset_index(drop=True)
