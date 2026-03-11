import pandas as pd
import streamlit as st

from streamlit_dashboard.services.postgres import (
    get_db_connection_error,
    get_quarantine_tables,
    get_table_preview,
)
from streamlit_dashboard.services.reports import get_report_bundle
from streamlit_dashboard.ui import (
    apply_app_style,
    render_dataframe_block,
    render_page_header,
    render_sidebar,
    render_status_box,
)


st.set_page_config(page_title="Quarantine Explorer", page_icon=":warning:", layout="wide")
apply_app_style()
render_sidebar()
render_page_header(
    "Quarantine Explorer",
    "Focus sur les jeux en echec et les raisons de quarantaine.",
)

db_error = get_db_connection_error()
if db_error:
    render_status_box(
        "warn",
        "Base non joignable",
        "La consultation des tables de quarantaine est indisponible.",
    )
    st.code(db_error)
else:
    tables_df = get_quarantine_tables()
    if tables_df.empty:
        render_status_box(
            "info",
            "Aucune table de quarantaine detectee",
            "Le schema public ne contient pas encore de table `quarantine`.",
        )
    else:
        selected_table = st.selectbox(
            "Table de quarantaine",
            tables_df["tablename"].tolist(),
        )
        max_rows = st.slider("Nombre max de lignes chargees", 50, 2000, 200, 50)
        search_query = st.text_input("Recherche texte")
        quarantine_df = get_table_preview(selected_table, limit=max_rows)

        if not quarantine_df.empty and "quarantine_reason" in quarantine_df.columns:
            available_reasons = sorted(
                value for value in quarantine_df["quarantine_reason"].dropna().unique().tolist()
            )
            selected_reasons = st.multiselect(
                "Filtrer par raison",
                available_reasons,
            )
            if selected_reasons:
                quarantine_df = quarantine_df.loc[
                    quarantine_df["quarantine_reason"].isin(selected_reasons)
                ]

        if search_query and not quarantine_df.empty:
            mask = quarantine_df.astype(str).apply(
                lambda column: column.str.contains(search_query, case=False, na=False)
            )
            quarantine_df = quarantine_df.loc[mask.any(axis=1)]

        if not quarantine_df.empty and "quarantine_reason" in quarantine_df.columns:
            st.markdown("### Top raisons dans la table selectionnee")
            reasons_df = (
                quarantine_df["quarantine_reason"]
                .fillna("")
                .loc[lambda series: series.ne("")]
                .value_counts()
                .rename_axis("quarantine_reason")
                .reset_index(name="row_count")
            )
            st.dataframe(reasons_df, use_container_width=True)
            st.bar_chart(reasons_df.set_index("quarantine_reason"))

        render_dataframe_block(
            "Apercu quarantaine",
            quarantine_df,
            "Aucune ligne disponible dans cette table.",
            f"{selected_table}_preview.csv",
        )

st.markdown("## Raisons agregees depuis les rapports Soda")
raw_col, processed_col = st.columns(2)
with raw_col:
    render_dataframe_block(
        "Raw reasons",
        get_report_bundle("raw")["reasons"],
        "Aucune raison raw disponible.",
        "raw_quarantine_reasons.csv",
    )
with processed_col:
    render_dataframe_block(
        "Processed reasons",
        get_report_bundle("processed")["reasons"],
        "Aucune raison processed disponible.",
        "processed_quarantine_reasons.csv",
    )
