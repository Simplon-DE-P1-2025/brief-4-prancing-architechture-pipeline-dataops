import os

import pandas as pd

from dags.chicago_pipeline.constants import (
    AGG_COMMUNITY_CSV_PATH,
    AGG_HOURLY_CSV_PATH,
    AGG_MONTHLY_CSV_PATH,
    AGGREGATED_CSV_PATH,
    AGG_SERIOUS_CSV_PATH,
    AGG_YEARLY_CSV_PATH,
    CLEAN_CSV_PATH,
    FILTERED_CSV_PATH,
    RAW_CSV_PATH,
)


def map_bool_column(series: pd.Series) -> pd.Series:
    return series.map({"true": True, "false": False, "True": True, "False": False})


def load_raw_dataframe() -> pd.DataFrame:
    return pd.read_csv(RAW_CSV_PATH, dtype=str)


def transform_filter(**_context):
    df = load_raw_dataframe()

    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["x_coordinate"] = pd.to_numeric(df["x_coordinate"], errors="coerce")
    df["y_coordinate"] = pd.to_numeric(df["y_coordinate"], errors="coerce")
    df["arrest"] = map_bool_column(df["arrest"])
    df["domestic"] = map_bool_column(df["domestic"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["updated_on"] = pd.to_datetime(df["updated_on"], errors="coerce")
    df.columns = [col.lower().strip() for col in df.columns]

    df.dropna(subset=["id", "date", "primary_type"], inplace=True)
    df.drop_duplicates(subset=["id"], inplace=True)

    os.makedirs(os.path.dirname(FILTERED_CSV_PATH), exist_ok=True)
    df.to_csv(FILTERED_CSV_PATH, index=False)


def transform_agg(**_context):
    df = load_raw_dataframe()

    df["arrest"] = map_bool_column(df["arrest"])
    df["domestic"] = map_bool_column(df["domestic"])
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    agg = (
        df.groupby(["primary_type", "district"])
        .agg(
            total_crimes=("id", "count"),
            total_arrests=("arrest", "sum"),
            total_domestic=("domestic", "sum"),
            year_min=("year", "min"),
            year_max=("year", "max"),
        )
        .reset_index()
    )

    agg["arrest_rate"] = (agg["total_arrests"] / agg["total_crimes"] * 100).round(2)

    os.makedirs(os.path.dirname(AGGREGATED_CSV_PATH), exist_ok=True)
    agg.to_csv(AGGREGATED_CSV_PATH, index=False)


def transform_agg_hourly(**_context):
    df = load_raw_dataframe()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.dropna(subset=["date"], inplace=True)
    df["hour"] = df["date"].dt.hour

    def get_time_slot(hour):
        if hour < 6:
            return "nuit (0h-6h)"
        if hour < 12:
            return "matin (6h-12h)"
        if hour < 18:
            return "apres-midi (12h-18h)"
        return "soir (18h-0h)"

    df["time_slot"] = df["hour"].apply(get_time_slot)

    agg = (
        df.groupby(["primary_type", "time_slot"])
        .agg(total_crimes=("id", "count"))
        .reset_index()
    )

    os.makedirs(os.path.dirname(AGG_HOURLY_CSV_PATH), exist_ok=True)
    agg.to_csv(AGG_HOURLY_CSV_PATH, index=False)
    print(f"Agregation horaire terminee: {len(agg)} lignes")


def transform_agg_monthly(**_context):
    df = load_raw_dataframe()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.dropna(subset=["date"], inplace=True)
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    agg = (
        df.groupby(["primary_type", "year", "month"])
        .agg(total_crimes=("id", "count"))
        .reset_index()
    )

    os.makedirs(os.path.dirname(AGG_MONTHLY_CSV_PATH), exist_ok=True)
    agg.to_csv(AGG_MONTHLY_CSV_PATH, index=False)
    print(f"Agregation mensuelle terminee: {len(agg)} lignes")


def transform_agg_serious(**_context):
    df = load_raw_dataframe()

    serious_types = [
        "HOMICIDE",
        "ASSAULT",
        "ROBBERY",
        "WEAPONS VIOLATION",
        "KIDNAPPING",
        "CRIMINAL SEXUAL ASSAULT",
    ]

    df = df[df["primary_type"].isin(serious_types)]
    df["arrest"] = map_bool_column(df["arrest"])
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    agg = (
        df.groupby(["primary_type", "district"])
        .agg(
            total_crimes=("id", "count"),
            total_arrests=("arrest", "sum"),
        )
        .reset_index()
    )

    agg["arrest_rate"] = (agg["total_arrests"] / agg["total_crimes"] * 100).round(2)

    os.makedirs(os.path.dirname(AGG_SERIOUS_CSV_PATH), exist_ok=True)
    agg.to_csv(AGG_SERIOUS_CSV_PATH, index=False)
    print(f"Agregation crimes graves terminee: {len(agg)} lignes")


def transform_agg_community(**_context):
    df = load_raw_dataframe()

    df.dropna(subset=["community_area"], inplace=True)
    df["arrest"] = map_bool_column(df["arrest"])
    df["domestic"] = map_bool_column(df["domestic"])

    agg = (
        df.groupby(["community_area"])
        .agg(
            total_crimes=("id", "count"),
            total_arrests=("arrest", "sum"),
            total_domestic=("domestic", "sum"),
        )
        .reset_index()
    )

    agg["arrest_rate"] = (agg["total_arrests"] / agg["total_crimes"] * 100).round(2)
    agg["domestic_rate"] = (agg["total_domestic"] / agg["total_crimes"] * 100).round(2)

    os.makedirs(os.path.dirname(AGG_COMMUNITY_CSV_PATH), exist_ok=True)
    agg.to_csv(AGG_COMMUNITY_CSV_PATH, index=False)
    print(f"Agregation par quartier terminee: {len(agg)} lignes")


def transform_agg_yearly(**_context):
    df = load_raw_dataframe()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df.dropna(subset=["year"], inplace=True)
    df["arrest"] = map_bool_column(df["arrest"])

    agg = (
        df.groupby(["year", "primary_type"])
        .agg(
            total_crimes=("id", "count"),
            total_arrests=("arrest", "sum"),
        )
        .reset_index()
    )

    agg["arrest_rate"] = (agg["total_arrests"] / agg["total_crimes"] * 100).round(2)

    os.makedirs(os.path.dirname(AGG_YEARLY_CSV_PATH), exist_ok=True)
    agg.to_csv(AGG_YEARLY_CSV_PATH, index=False)
    print(f"Agregation annuelle terminee: {len(agg)} lignes")


def merge_and_finalize(**_context):
    df = pd.read_csv(FILTERED_CSV_PATH)
    os.makedirs(os.path.dirname(CLEAN_CSV_PATH), exist_ok=True)
    df.to_csv(CLEAN_CSV_PATH, index=False)
