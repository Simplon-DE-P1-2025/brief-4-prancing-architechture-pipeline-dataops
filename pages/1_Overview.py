import pandas as pd
import streamlit as st

from streamlit_dashboard.services.postgres import get_db_connection_error, get_table_row_counts
from streamlit_dashboard.services.reports import (
    extract_summary_metric,
    get_report_bundle,
    get_report_last_updated,
    get_report_status,
)
from streamlit_dashboard.ui import (
    apply_app_style,
    render_dataframe_block,
    render_health_cards,
    render_kpi_cards,
    render_page_header,
    render_sidebar,
    render_status_box,
)


st.set_page_config(page_title="Overview", page_icon=":bar_chart:", layout="wide")
apply_app_style()
render_sidebar()
render_page_header(
    "Overview",
    "KPIs principaux du pipeline et volumetrie de la base PostgreSQL.",
)

db_error = get_db_connection_error()
if db_error:
    render_status_box(
        "warn",
        "Connexion PostgreSQL indisponible",
        "Les rapports Soda restent visibles, mais la partie DB est degradee.",
    )

raw_bundle = get_report_bundle("raw")
processed_bundle = get_report_bundle("processed")

render_health_cards(
    [
        {
            "label": "Raw",
            "status": get_report_status("raw")[0],
            "caption": f"{get_report_status('raw')[1]} · {get_report_last_updated('raw')}",
        },
        {
            "label": "Processed",
            "status": get_report_status("processed")[0],
            "caption": f"{get_report_status('processed')[1]} · {get_report_last_updated('processed')}",
        },
        {
            "label": "Database",
            "status": "warn" if db_error else "ok",
            "caption": "Connexion PostgreSQL",
        },
    ]
)

render_kpi_cards(
    [
        {
            "label": "Raw valid ratio",
            "value": extract_summary_metric(raw_bundle["summary"], "valid_ratio"),
            "caption": "Part du brut conservee apres validation",
        },
        {
            "label": "Processed valid ratio",
            "value": extract_summary_metric(processed_bundle["summary"], "valid_ratio"),
            "caption": "Part du dataset final avant chargement",
        },
        {
            "label": "Raw contract",
            "value": extract_summary_metric(raw_bundle["summary"], "contract_passed"),
            "caption": "Statut du contrat raw",
        },
        {
            "label": "Processed contract",
            "value": extract_summary_metric(processed_bundle["summary"], "contract_passed"),
            "caption": "Statut du contrat processed",
        },
    ]
)

counts_df = get_table_row_counts()

st.markdown("### Perte de volume")
volume_loss_df = pd.DataFrame(
    [
        {"stage": "raw_total", "rows": extract_summary_metric(raw_bundle["summary"], "total_rows")},
        {"stage": "raw_valid", "rows": extract_summary_metric(raw_bundle["summary"], "valid_rows")},
        {"stage": "processed_total", "rows": extract_summary_metric(processed_bundle["summary"], "total_rows")},
        {"stage": "processed_valid", "rows": extract_summary_metric(processed_bundle["summary"], "valid_rows")},
    ]
)
volume_loss_df["rows"] = pd.to_numeric(volume_loss_df["rows"], errors="coerce")
st.dataframe(volume_loss_df, use_container_width=True)
st.bar_chart(volume_loss_df.set_index("stage"))

render_dataframe_block(
    "Volumetrie des tables",
    counts_df,
    "Aucune information de volumetrie disponible.",
    "table_row_counts.csv",
)

if not counts_df.empty:
    st.markdown("### Repartition visuelle")
    st.bar_chart(counts_df.set_index("table_name"))
