import os
from pathlib import Path
from typing import Any

import yaml


BASE_DIR = Path(__file__).resolve().parent.parent
REPORTS_DIR = BASE_DIR / "include" / "data" / "reports"
HISTORY_DIR = REPORTS_DIR / "history"
SODA_CONFIG_PATH = BASE_DIR / "include" / "soda" / "configuration.yml"

REPORT_DEFINITIONS = {
    "raw": {
        "label": "Raw",
        "summary": REPORTS_DIR / "raw_quality_summary.csv",
        "reasons": REPORTS_DIR / "raw_quality_reasons.csv",
        "markdown": REPORTS_DIR / "raw_quality_report.md",
    },
    "processed": {
        "label": "Processed",
        "summary": REPORTS_DIR / "processed_quality_summary.csv",
        "reasons": REPORTS_DIR / "processed_quality_reasons.csv",
        "markdown": REPORTS_DIR / "processed_quality_report.md",
    },
}


def load_soda_connection_config() -> dict[str, Any]:
    connection: dict[str, Any] = {}
    if SODA_CONFIG_PATH.exists():
        config = yaml.safe_load(SODA_CONFIG_PATH.read_text(encoding="utf-8")) or {}
        connection = config.get("connection", {}) or {}

    return {
        "host": os.getenv("STREAMLIT_DB_HOST", connection.get("host", "postgres")),
        "port": int(os.getenv("STREAMLIT_DB_PORT", connection.get("port", 5432))),
        "user": os.getenv("STREAMLIT_DB_USER", connection.get("user", "postgres")),
        "password": os.getenv(
            "STREAMLIT_DB_PASSWORD",
            connection.get("password", "postgres"),
        ),
        "database": os.getenv(
            "STREAMLIT_DB_NAME",
            connection.get("database", "chicago_crimes"),
        ),
    }
