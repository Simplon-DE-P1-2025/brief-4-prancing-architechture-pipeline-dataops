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
def table_exists(table_name: str) -> bool:
    df = _query_dataframe(
        """
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public' AND tablename = %s
        """,
        [table_name],
    )
    return not df.empty


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
def get_table_data(table_name: str, limit: int | None = None) -> pd.DataFrame:
    query = f'SELECT * FROM public."{table_name}"'
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    return _query_dataframe(query)


@st.cache_data(show_spinner=False, ttl=30)
def get_table_preview(table_name: str, limit: int = 100) -> pd.DataFrame:
    return get_table_data(table_name, limit=limit)


@st.cache_data(show_spinner=False, ttl=30)
def get_quarantine_tables() -> pd.DataFrame:
    tables_df = get_public_tables()
    if tables_df.empty:
        return pd.DataFrame()
    return tables_df.loc[
        tables_df["tablename"].str.contains("quarantine", case=False, na=False)
    ].reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=30)
def get_table_snapshot(
    table_name: str,
    freshness_column: str | None = None,
    date_column: str | None = None,
) -> dict[str, Any]:
    if not table_exists(table_name):
        return {}

    select_clauses = ['COUNT(*)::bigint AS row_count']
    if freshness_column:
        select_clauses.append(f'MAX("{freshness_column}") AS freshness_at')
    if date_column:
        select_clauses.append(f'MIN("{date_column}") AS min_date')
        select_clauses.append(f'MAX("{date_column}") AS max_date')

    df = _query_dataframe(
        f'SELECT {", ".join(select_clauses)} FROM public."{table_name}"'
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


@st.cache_data(show_spinner=False, ttl=30)
def get_category_breakdown(
    table_name: str,
    column_name: str = "primary_type",
    metric_column: str = "*",
    limit: int = 8,
) -> pd.DataFrame:
    if not table_exists(table_name):
        return pd.DataFrame()

    metric_expression = "COUNT(*)" if metric_column == "*" else f'SUM("{metric_column}")'
    return _query_dataframe(
        f'''
        SELECT "{column_name}" AS category, {metric_expression} AS value
        FROM public."{table_name}"
        WHERE "{column_name}" IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {int(limit)}
        '''
    )


@st.cache_data(show_spinner=False, ttl=30)
def get_quarantine_reason_breakdown(limit: int = 8) -> pd.DataFrame:
    if not table_exists("chicago_crimes_quarantine"):
        return pd.DataFrame()

    return _query_dataframe(
        f"""
        WITH expanded AS (
            SELECT trim(reason) AS quarantine_reason
            FROM public."chicago_crimes_quarantine",
            unnest(string_to_array(coalesce(quarantine_reason, ''), '|')) AS reason
            WHERE coalesce(quarantine_reason, '') <> ''
        )
        SELECT quarantine_reason, COUNT(*) AS row_count
        FROM expanded
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {int(limit)}
        """
    )


@st.cache_data(show_spinner=False, ttl=30)
def get_quarantine_column_breakdown(limit: int = 8) -> pd.DataFrame:
    if not table_exists("chicago_crimes_quarantine"):
        return pd.DataFrame()

    return _query_dataframe(
        f"""
        WITH expanded AS (
            SELECT trim(reason) AS quarantine_reason
            FROM public."chicago_crimes_quarantine",
            unnest(string_to_array(coalesce(quarantine_reason, ''), '|')) AS reason
            WHERE coalesce(quarantine_reason, '') <> ''
        )
        SELECT
            regexp_replace(quarantine_reason, '_(missing|duplicate|invalid)$', '') AS column_name,
            COUNT(*) AS row_count
        FROM expanded
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {int(limit)}
        """
    )
