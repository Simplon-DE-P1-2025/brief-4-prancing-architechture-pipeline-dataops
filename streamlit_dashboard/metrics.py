import math

import pandas as pd


def to_float(value) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value) -> int | None:
    numeric = to_float(value)
    if numeric is None:
        return None
    return int(numeric)


def safe_ratio(numerator, denominator) -> float | None:
    num = to_float(numerator)
    den = to_float(denominator)
    if num is None or den in (None, 0):
        return None
    return num / den


def format_number(value) -> str:
    numeric = to_float(value)
    if numeric is None:
        return "-"
    if math.isfinite(numeric) and numeric.is_integer():
        return f"{int(numeric):,}".replace(",", " ")
    return f"{numeric:,.1f}".replace(",", " ")


def format_percent(value, decimals: int = 1) -> str:
    numeric = to_float(value)
    if numeric is None:
        return "-"
    if numeric <= 1:
        numeric *= 100
    return f"{numeric:.{decimals}f}%"


def format_timestamp(value) -> str:
    if value in (None, "", "-"):
        return "-"
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return "-"
    return timestamp.strftime("%Y-%m-%d %H:%M")


def format_date(value) -> str:
    if value in (None, "", "-"):
        return "-"
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return "-"
    return timestamp.strftime("%Y-%m-%d")


def describe_freshness(value) -> str:
    if value in (None, "", "-"):
        return "-"
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return "-"

    now = pd.Timestamp.now(tz=timestamp.tz) if timestamp.tzinfo else pd.Timestamp.now()
    delta = now - timestamp
    total_hours = int(delta.total_seconds() // 3600)
    total_days = int(delta.total_seconds() // 86400)

    if total_hours < 1:
        return "moins d'1h"
    if total_hours < 24:
        return f"{total_hours}h"
    return f"{total_days}j"
