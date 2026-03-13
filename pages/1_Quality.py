import streamlit as st

from streamlit_dashboard.charts import ALERT, bar_chart
from streamlit_dashboard.metrics import format_number, format_percent
from streamlit_dashboard.services.reports import get_report_overview
from streamlit_dashboard.ui import (
    apply_app_style,
    render_metric_cards,
    render_page_header,
    render_section_header,
    render_sidebar,
    render_spacer,
    render_status_cards,
)


apply_app_style()
render_sidebar()
render_page_header(
    "Quality Monitor",
    "Page dediee au suivi des controles Soda, des volumes verifies et des principales causes de quarantaine.",
    "Quality",
)

raw = get_report_overview("raw")
processed = get_report_overview("processed")

left_col, right_col = st.columns(2, gap="large")

for col, title, overview in [
    (left_col, "Raw", raw),
    (right_col, "Processed", processed),
]:
    with col:
        render_section_header(
            title,
            "Statut du contrat, volumetrie controlee et principales raisons de quarantaine.",
        )
        render_status_cards(
            [
                {
                    "label": f"{title} contract",
                    "value": overview["status_label"],
                    "status": overview["status"],
                    "caption": f"Dernier scan {overview['last_updated']}",
                }
            ],
            columns=1,
        )
        render_spacer("0.9rem")
        render_metric_cards(
            [
                {
                    "label": "Rows checked",
                    "value": format_number(overview["total_rows"]),
                    "caption": "Volume evalue par le contrat",
                    "tone": overview["status"],
                },
                {
                    "label": "Rows valid",
                    "value": format_number(overview["valid_rows"]),
                    "caption": "Volume conserve",
                    "tone": "ok",
                },
                {
                    "label": "Rows quarantined",
                    "value": format_number(overview["quarantine_rows"]),
                    "caption": "Volume ecarte",
                    "tone": "warn",
                },
                {
                    "label": "Valid ratio",
                    "value": format_percent(overview["valid_ratio"]),
                    "caption": f"Contract passed: {overview['contract_passed']}",
                    "tone": overview["status"],
                },
            ],
            columns=2,
        )
        render_spacer("0.9rem")
        reasons_df = overview["reasons"]
        if not reasons_df.empty:
            st.altair_chart(
                bar_chart(
                    reasons_df,
                    "row_count",
                    "quarantine_reason",
                    color=ALERT,
                    horizontal=True,
                    height=320,
                    x_title="Nombre de lignes en quarantaine",
                    y_title="Raisons de quarantaine",
                ),
                use_container_width=True,
            )
        else:
            st.info("Aucune raison de quarantaine disponible.")
