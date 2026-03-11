import streamlit as st

from streamlit_dashboard.services.postgres import (
    get_db_connection_error,
    get_public_tables,
    get_table_columns,
    get_table_preview,
)
from streamlit_dashboard.ui import (
    apply_app_style,
    render_dataframe_block,
    render_page_header,
    render_sidebar,
    render_status_box,
)


st.set_page_config(page_title="Database Explorer", page_icon=":card_file_box:", layout="wide")
apply_app_style()
render_sidebar()
render_page_header(
    "Database Explorer",
    "Exploration des tables publiques chargees par le pipeline.",
)

db_error = get_db_connection_error()
if db_error:
    render_status_box(
        "warn",
        "Base non joignable",
        "Verifie la configuration Docker ou les variables STREAMLIT_DB_*.",
    )
    st.code(db_error)
    st.stop()

tables_df = get_public_tables()
if tables_df.empty:
    render_status_box(
        "info",
        "Aucune table detectee",
        "Le pipeline n'a pas encore charge de table dans le schema public.",
    )
    st.stop()

table_name = st.selectbox("Table publique", tables_df["tablename"].tolist())
limit = st.slider("Nombre de lignes d'apercu", min_value=10, max_value=500, value=100, step=10)

columns_col, preview_col = st.columns([1, 2])
with columns_col:
    render_dataframe_block(
        "Colonnes",
        get_table_columns(table_name),
        "Aucune colonne disponible.",
        f"{table_name}_columns.csv",
    )
with preview_col:
    render_dataframe_block(
        "Apercu",
        get_table_preview(table_name, limit=limit),
        "Aucune ligne disponible.",
        f"{table_name}_preview.csv",
    )
