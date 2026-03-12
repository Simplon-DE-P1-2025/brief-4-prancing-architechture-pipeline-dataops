import os

import pandas as pd

from dags.chicago_pipeline.constants import (
    AGG_COMMUNITY_CSV_PATH,
    AGG_HOURLY_CSV_PATH,
    AGG_MONTHLY_CSV_PATH,
    AGG_SERIOUS_CSV_PATH,
    AGG_YEARLY_CSV_PATH,
    PROCESSED_QUARANTINE_CSV_PATH,
    PROCESSED_VALID_CSV_PATH,
    QUARANTINE_CSV_PATH,
)
from dags.chicago_pipeline.database import replace_table_from_dataframe


def load_valid_data(**_context):
    df_valid = pd.read_csv(PROCESSED_VALID_CSV_PATH)
    replace_table_from_dataframe(df_valid, "chicago_crimes")


def load_quarantine_data(**_context):
    df_quarantine = pd.read_csv(PROCESSED_QUARANTINE_CSV_PATH)
    os.makedirs(os.path.dirname(QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(QUARANTINE_CSV_PATH, index=False)
    replace_table_from_dataframe(df_quarantine, "chicago_crimes_quarantine")


def load_agg_hourly(**_context):
    df = pd.read_csv(AGG_HOURLY_CSV_PATH)
    replace_table_from_dataframe(df, "chicago_crimes_agg_hourly")
    print(f"Agregation horaire chargee: {len(df)} lignes")


def load_agg_monthly(**_context):
    df = pd.read_csv(AGG_MONTHLY_CSV_PATH)
    replace_table_from_dataframe(df, "chicago_crimes_agg_monthly")
    print(f"Agregation mensuelle chargee: {len(df)} lignes")


def load_agg_serious(**_context):
    df = pd.read_csv(AGG_SERIOUS_CSV_PATH)
    replace_table_from_dataframe(df, "chicago_crimes_agg_serious")
    print(f"Agregation crimes graves chargee: {len(df)} lignes")


def load_agg_community(**_context):
    df = pd.read_csv(AGG_COMMUNITY_CSV_PATH)
    replace_table_from_dataframe(df, "chicago_crimes_agg_community")
    print(f"Agregation par quartier chargee: {len(df)} lignes")


def load_agg_yearly(**_context):
    df = pd.read_csv(AGG_YEARLY_CSV_PATH)
    replace_table_from_dataframe(df, "chicago_crimes_agg_yearly")
    print(f"Agregation annuelle chargee: {len(df)} lignes")
