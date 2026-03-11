import streamlit as st

from streamlit_dashboard.services.reports import (
    extract_summary_metric,
    get_report_bundle,
    get_report_last_updated,
)
from streamlit_dashboard.ui import (
    apply_app_style,
    render_dataframe_block,
    render_kpi_cards,
    render_page_header,
    render_sidebar,
)


st.set_page_config(page_title="Soda Reports", page_icon=":clipboard:", layout="wide")
apply_app_style()
render_sidebar()
render_page_header(
    "Soda Reports",
    "Consultation detaillee des validations raw et processed.",
)

raw_tab, processed_tab = st.tabs(["Raw", "Processed"])

for tab, report_key in [(raw_tab, "raw"), (processed_tab, "processed")]:
    bundle = get_report_bundle(report_key)
    with tab:
        st.caption(f"Derniere mise a jour: {get_report_last_updated(report_key)}")
        render_kpi_cards(
            [
                {
                    "label": "Contract passed",
                    "value": extract_summary_metric(bundle["summary"], "contract_passed"),
                    "caption": "Statut global du contrat",
                },
                {
                    "label": "Total rows",
                    "value": extract_summary_metric(bundle["summary"], "total_rows"),
                    "caption": "Volume evalue",
                },
                {
                    "label": "Valid rows",
                    "value": extract_summary_metric(bundle["summary"], "valid_rows"),
                    "caption": "Lignes conservees",
                },
                {
                    "label": "Quarantine rows",
                    "value": extract_summary_metric(bundle["summary"], "quarantine_rows"),
                    "caption": "Lignes ecartees",
                },
            ]
        )

        render_dataframe_block(
            "Summary",
            bundle["summary"],
            "Aucun fichier summary disponible.",
            f"{report_key}_summary.csv",
        )
        render_dataframe_block(
            "Reasons",
            bundle["reasons"],
            "Aucune raison de quarantaine disponible.",
            f"{report_key}_reasons.csv",
        )

        if not bundle["reasons"].empty:
            st.markdown("### Chart")
            st.bar_chart(bundle["reasons"].set_index("quarantine_reason"))

        st.markdown("### Rapport markdown")
        if bundle["markdown"]:
            st.markdown(bundle["markdown"])
        else:
            st.info("Aucun rapport Markdown genere.")
