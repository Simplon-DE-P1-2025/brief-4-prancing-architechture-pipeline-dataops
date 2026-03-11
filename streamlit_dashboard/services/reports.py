from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from streamlit_dashboard.config import HISTORY_DIR, REPORTS_DIR, REPORT_DEFINITIONS


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _read_markdown_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


@st.cache_data(show_spinner=False, ttl=30)
def get_report_bundle(report_key: str) -> dict[str, Any]:
    definition = REPORT_DEFINITIONS[report_key]
    return {
        "summary": _read_csv_if_exists(definition["summary"]),
        "reasons": _read_csv_if_exists(definition["reasons"]),
        "markdown": _read_markdown_if_exists(definition["markdown"]),
        "paths": definition,
    }


@st.cache_data(show_spinner=False, ttl=30)
def list_report_files() -> list[str]:
    if not REPORTS_DIR.exists():
        return []
    return sorted(path.name for path in REPORTS_DIR.iterdir() if path.is_file())


def extract_summary_metric(summary_df: pd.DataFrame, metric: str) -> str:
    if summary_df.empty:
        return "-"
    match = summary_df.loc[summary_df["metric"] == metric, "value"]
    if match.empty:
        return "-"
    return str(match.iloc[0])


def _extract_summary_metric_float(summary_df: pd.DataFrame, metric: str) -> float | None:
    value = extract_summary_metric(summary_df, metric)
    if value == "-":
        return None
    try:
        return float(value)
    except ValueError:
        return None


@st.cache_data(show_spinner=False, ttl=30)
def get_report_last_updated(report_key: str) -> str:
    path = REPORT_DEFINITIONS[report_key]["summary"]
    if not path.exists():
        return "-"
    return pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")


@st.cache_data(show_spinner=False, ttl=30)
def get_report_history(report_key: str) -> pd.DataFrame:
    history_path = HISTORY_DIR / f"{report_key}_history.csv"
    if history_path.exists():
        return pd.read_csv(history_path)

    bundle = get_report_bundle(report_key)
    summary_df = bundle["summary"]
    if summary_df.empty:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "snapshot_at": get_report_last_updated(report_key),
                "report_key": report_key,
                "total_rows": _extract_summary_metric_float(summary_df, "total_rows"),
                "valid_rows": _extract_summary_metric_float(summary_df, "valid_rows"),
                "quarantine_rows": _extract_summary_metric_float(summary_df, "quarantine_rows"),
                "valid_ratio": _extract_summary_metric_float(summary_df, "valid_ratio"),
            }
        ]
    )


def get_report_status(report_key: str) -> tuple[str, str]:
    bundle = get_report_bundle(report_key)
    summary_df = bundle["summary"]
    if summary_df.empty:
        return ("info", "Rapport absent")

    passed = extract_summary_metric(summary_df, "contract_passed").lower()
    quarantine_rows = _extract_summary_metric_float(summary_df, "quarantine_rows") or 0
    if passed == "true" and quarantine_rows == 0:
        return ("ok", "Sain")
    if passed == "true":
        return ("warn", "Avec quarantaine")
    return ("warn", "Contrat en echec")
