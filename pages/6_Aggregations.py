import streamlit as st

from streamlit_dashboard.services.postgres import (
    get_db_connection_error,
    get_table_preview,
)
from streamlit_dashboard.ui import (
    apply_app_style,
    render_page_header,
    render_sidebar,
    render_status_box,
)


st.set_page_config(page_title="Aggregations", page_icon=":bar_chart:", layout="wide")
apply_app_style()
render_sidebar()
render_page_header(
    "Aggregations",
    "Visualisation des 5 agregations ajoutees au pipeline (hourly, monthly, serious, community, yearly).",
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

# --- Agregation horaire ---
st.markdown("### Crimes par tranche horaire et type")
df_hourly = get_table_preview("chicago_crimes_agg_hourly", limit=500)
if not df_hourly.empty:
    # Colonnes: primary_type, time_slot, total_crimes
    by_slot = df_hourly.groupby("time_slot")["total_crimes"].sum().sort_index()
    st.bar_chart(by_slot)
    with st.expander("Voir les donnees"):
        st.dataframe(df_hourly, use_container_width=True)
else:
    st.info("Table chicago_crimes_agg_hourly non disponible.")

# --- Agregation mensuelle ---
st.markdown("### Crimes par mois")
df_monthly = get_table_preview("chicago_crimes_agg_monthly", limit=500)
if not df_monthly.empty:
    # Colonnes: primary_type, year, month, total_crimes
    by_month = df_monthly.groupby("month")["total_crimes"].sum().sort_index()
    st.bar_chart(by_month)
    with st.expander("Voir les donnees"):
        st.dataframe(df_monthly, use_container_width=True)
else:
    st.info("Table chicago_crimes_agg_monthly non disponible.")

# --- Agregation crimes graves ---
st.markdown("### Crimes graves par type et district")
df_serious = get_table_preview("chicago_crimes_agg_serious", limit=500)
if not df_serious.empty:
    # Colonnes: primary_type, district, total_crimes, total_arrests
    by_type = df_serious.groupby("primary_type")["total_crimes"].sum().sort_values(ascending=False)
    st.bar_chart(by_type)
    with st.expander("Voir les donnees"):
        st.dataframe(df_serious, use_container_width=True)
else:
    st.info("Table chicago_crimes_agg_serious non disponible.")

# --- Agregation par communaute ---
st.markdown("### Crimes par zone communautaire (top 20)")
df_community = get_table_preview("chicago_crimes_agg_community", limit=500)
if not df_community.empty:
    # Colonnes: community_area, total_crimes, total_arrests, total_domestic, arrest_rate, domestic_rate
    top20 = df_community.sort_values("total_crimes", ascending=False).head(20)
    st.bar_chart(top20.set_index("community_area")["total_crimes"])
    with st.expander("Voir toutes les donnees"):
        st.dataframe(df_community, use_container_width=True)
else:
    st.info("Table chicago_crimes_agg_community non disponible.")

# --- Agregation annuelle ---
st.markdown("### Evolution annuelle des crimes")
df_yearly = get_table_preview("chicago_crimes_agg_yearly", limit=500)
if not df_yearly.empty:
    # Colonnes: year, primary_type, total_crimes, total_arrests, arrest_rate
    by_year = df_yearly.groupby("year")["total_crimes"].sum().sort_index()
    st.line_chart(by_year)
    with st.expander("Voir les donnees"):
        st.dataframe(df_yearly, use_container_width=True)
else:
    st.info("Table chicago_crimes_agg_yearly non disponible.")
