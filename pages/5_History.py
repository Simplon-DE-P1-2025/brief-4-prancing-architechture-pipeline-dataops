import streamlit as st

from streamlit_dashboard.services.reports import get_report_history
from streamlit_dashboard.ui import (
    apply_app_style,
    render_dataframe_block,
    render_page_header,
    render_sidebar,
    render_status_box,
)


st.set_page_config(page_title="History", page_icon=":clock3:", layout="wide")
apply_app_style()
render_sidebar()
render_page_header(
    "History",
    "Evolution des snapshots de qualite si un historique est disponible.",
)

raw_history = get_report_history("raw")
processed_history = get_report_history("processed")

if len(raw_history) <= 1 and len(processed_history) <= 1:
    render_status_box(
        "info",
        "Historique limite",
        "Seul le snapshot courant est disponible. Si tu ajoutes des fichiers `*_history.csv`, cette page affichera les tendances.",
    )

raw_col, processed_col = st.columns(2)
with raw_col:
    render_dataframe_block(
        "Raw history",
        raw_history,
        "Aucun historique raw disponible.",
        "raw_history.csv",
    )
    if not raw_history.empty and "snapshot_at" in raw_history.columns:
        st.line_chart(raw_history.set_index("snapshot_at")[["valid_rows", "quarantine_rows"]])

with processed_col:
    render_dataframe_block(
        "Processed history",
        processed_history,
        "Aucun historique processed disponible.",
        "processed_history.csv",
    )
    if not processed_history.empty and "snapshot_at" in processed_history.columns:
        st.line_chart(
            processed_history.set_index("snapshot_at")[["valid_rows", "quarantine_rows"]]
        )
