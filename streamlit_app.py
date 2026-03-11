import pandas as pd
import streamlit as st

from streamlit_dashboard.services.postgres import get_db_connection_error, get_table_row_counts
from streamlit_dashboard.services.reports import (
    extract_summary_metric,
    get_report_bundle,
    get_report_last_updated,
    get_report_status,
    list_report_files,
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


st.set_page_config(
    page_title="Chicago Crimes Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

apply_app_style()
render_sidebar()
render_page_header(
    "Chicago Crimes Quality Dashboard",
    "Suivi local des validations Soda, des quarantaines et de la volumetrie PostgreSQL.",
)

db_error = get_db_connection_error()
if db_error:
    render_status_box(
        "warn",
        "PostgreSQL indisponible",
        "L'application reste consultable, mais la volumetrie et l'exploration des tables ne sont pas disponibles.",
    )
else:
    render_status_box(
        "ok",
        "PostgreSQL joignable",
        "Les onglets de volumetrie et d'exploration base sont disponibles.",
    )

raw_bundle = get_report_bundle("raw")
processed_bundle = get_report_bundle("processed")
counts_df = get_table_row_counts()

render_health_cards(
    [
        {
            "label": "Etat raw",
            "status": get_report_status("raw")[0],
            "caption": f"{get_report_status('raw')[1]} · maj {get_report_last_updated('raw')}",
        },
        {
            "label": "Etat processed",
            "status": get_report_status("processed")[0],
            "caption": f"{get_report_status('processed')[1]} · maj {get_report_last_updated('processed')}",
        },
        {
            "label": "Etat DB",
            "status": "warn" if db_error else "ok",
            "caption": "Connexion PostgreSQL",
        },
    ]
)

render_kpi_cards(
    [
        {
            "label": "Raw total",
            "value": extract_summary_metric(raw_bundle["summary"], "total_rows"),
            "caption": "Volume controle sur le contrat raw",
        },
        {
            "label": "Raw quarantine",
            "value": extract_summary_metric(raw_bundle["summary"], "quarantine_rows"),
            "caption": "Lignes ecartees apres le premier controle",
        },
        {
            "label": "Processed total",
            "value": extract_summary_metric(processed_bundle["summary"], "total_rows"),
            "caption": "Volume controle sur le contrat processed",
        },
        {
            "label": "Processed quarantine",
            "value": extract_summary_metric(processed_bundle["summary"], "quarantine_rows"),
            "caption": "Lignes ecartees avant chargement final",
        },
    ]
)

left_col, right_col = st.columns([1.2, 1])

with left_col:
    volume_story = st.session_state.get("volume_story")
    if volume_story is None:
        raw_total = extract_summary_metric(raw_bundle["summary"], "total_rows")
        raw_valid = extract_summary_metric(raw_bundle["summary"], "valid_rows")
        processed_total = extract_summary_metric(processed_bundle["summary"], "total_rows")
        processed_valid = extract_summary_metric(processed_bundle["summary"], "valid_rows")
        loaded_total = "-"
        if not counts_df.empty:
            match = counts_df.loc[counts_df["table_name"] == "chicago_crimes", "row_count"]
            if not match.empty:
                loaded_total = str(match.iloc[0])
        st.session_state["volume_story"] = [
            {"stage": "raw_total", "rows": raw_total},
            {"stage": "raw_valid", "rows": raw_valid},
            {"stage": "processed_total", "rows": processed_total},
            {"stage": "processed_valid", "rows": processed_valid},
            {"stage": "loaded_final", "rows": loaded_total},
        ]

    st.markdown("### Parcours des volumes")
    volume_story_df = pd.DataFrame(st.session_state["volume_story"])
    st.dataframe(volume_story_df, use_container_width=True)
    numeric_story_df = volume_story_df.copy()
    numeric_story_df["rows"] = pd.to_numeric(numeric_story_df["rows"], errors="coerce")
    st.bar_chart(numeric_story_df.set_index("stage"))

    render_dataframe_block(
        "Volumetrie PostgreSQL",
        counts_df,
        "Aucune volumetrie disponible pour le moment.",
        "postgres_row_counts.csv",
    )

with right_col:
    st.markdown("### Top anomalies")
    anomalies = []
    for report_key, bundle in [("raw", raw_bundle), ("processed", processed_bundle)]:
        reasons_df = bundle["reasons"]
        if reasons_df.empty:
            continue
        top_row = reasons_df.sort_values("row_count", ascending=False).iloc[0]
        anomalies.append(
            {
                "report": report_key,
                "reason": top_row["quarantine_reason"],
                "row_count": top_row["row_count"],
            }
        )
    if anomalies:
        import pandas as pd
        anomalies_df = pd.DataFrame(anomalies)
        st.dataframe(anomalies_df, use_container_width=True)
    else:
        st.info("Aucune anomalie principale detectee dans les rapports courants.")

    st.markdown("### Rapports disponibles")
    report_files = list_report_files()
    if not report_files:
        st.info("Aucun rapport detecte dans `include/data/reports/`.")
    else:
        for report_file in report_files:
            st.markdown(f"- `{report_file}`")

    st.markdown("### Navigation")
    st.markdown("- `Overview` pour les KPIs et la volumetrie")
    st.markdown("- `Soda Reports` pour le detail raw et processed")
    st.markdown("- `Database Explorer` pour parcourir les tables")
    st.markdown("- `Quarantine Explorer` pour se concentrer sur les tables en echec")
    st.markdown("- `History` pour les tendances si un historique est disponible")
