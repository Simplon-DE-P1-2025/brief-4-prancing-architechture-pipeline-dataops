import pandas as pd
import streamlit as st

from streamlit_dashboard.charts import ACCENT, NEUTRAL, OK, WARN, area_chart, bar_chart, line_chart
from streamlit_dashboard.metrics import format_number
from streamlit_dashboard.services.postgres import get_db_connection_error, get_table_data
from streamlit_dashboard.ui import (
    apply_app_style,
    render_metric_cards,
    render_page_header,
    render_section_header,
    render_sidebar,
    render_status_box,
)


apply_app_style()
render_sidebar()
render_page_header(
    "Insights",
    "Lecture analytique simple des sorties agrégées du pipeline, avec peu de graphes mais une meilleure finition.",
    "Insights",
)

db_error = get_db_connection_error()
if db_error:
    render_status_box(
        "warn",
        "PostgreSQL indisponible",
        "Les graphes d'agrégation ne peuvent pas etre construits sans acces aux tables finales.",
    )
    st.code(db_error)
    st.stop()

df_hourly = get_table_data("chicago_crimes_agg_hourly")
df_monthly = get_table_data("chicago_crimes_agg_monthly")
df_serious = get_table_data("chicago_crimes_agg_serious")
df_community = get_table_data("chicago_crimes_agg_community")
df_yearly = get_table_data("chicago_crimes_agg_yearly")

render_metric_cards(
    [
        {
            "label": "Hourly rows",
            "value": format_number(len(df_hourly)),
            "caption": "Tranches horaires",
            "tone": "neutral",
        },
        {
            "label": "Monthly rows",
            "value": format_number(len(df_monthly)),
            "caption": "Saisonnalite",
            "tone": "neutral",
        },
        {
            "label": "Community rows",
            "value": format_number(len(df_community)),
            "caption": "Zones communautaires",
            "tone": "neutral",
        },
        {
            "label": "Yearly rows",
            "value": format_number(len(df_yearly)),
            "caption": "Tendance annuelle",
            "tone": "neutral",
        },
    ]
)

top_row, middle_row = st.columns(2)

with top_row:
    render_section_header("Pattern horaire", "Vue simple des plages horaires les plus exposées.")
    if not df_hourly.empty:
        slot_order = {
            "nuit (0h-6h)": 0,
            "matin (6h-12h)": 1,
            "apres-midi (12h-18h)": 2,
            "soir (18h-0h)": 3,
        }
        hourly_plot = (
            df_hourly.groupby("time_slot", as_index=False)["total_crimes"]
            .sum()
            .assign(slot_rank=lambda frame: frame["time_slot"].map(slot_order))
            .sort_values("slot_rank")
        )
        st.altair_chart(
            bar_chart(
                hourly_plot,
                "time_slot",
                "total_crimes",
                color=ACCENT,
                sort=None,
                height=300,
                x_title="Tranches horaires",
                y_title="Nombre de crimes",
                x_label_angle=0,
            ),
            use_container_width=True,
        )
    else:
        st.info("Table horaire non disponible.")

with middle_row:
    render_section_header("Saisonnalite", "Evolution mensuelle agrégée de la volumétrie.")
    if not df_monthly.empty:
        monthly_plot = (
            df_monthly.groupby(["year", "month"], as_index=False)["total_crimes"]
            .sum()
            .assign(period=lambda frame: pd.to_datetime(dict(year=frame["year"], month=frame["month"], day=1)))
            .sort_values("period")
        )
        st.altair_chart(
            area_chart(
                monthly_plot,
                "period",
                "total_crimes",
                color=WARN,
                height=300,
                x_title="Periode",
                y_title="Nombre de crimes",
            ),
            use_container_width=True,
        )
    else:
        st.info("Table mensuelle non disponible.")

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    render_section_header("Crimes graves", "Types de crimes graves dominants sur la sortie agrégée.")
    if not df_serious.empty:
        serious_plot = (
            df_serious.groupby("primary_type", as_index=False)["total_crimes"]
            .sum()
            .sort_values("total_crimes", ascending=False)
        )
        st.altair_chart(
            bar_chart(
                serious_plot,
                "total_crimes",
                "primary_type",
                color="#A34F38",
                horizontal=True,
                height=300,
                x_title="Nombre de crimes",
                y_title="Type",
            ),
            use_container_width=True,
        )
    else:
        st.info("Table crimes graves non disponible.")

with bottom_right:
    render_section_header("Tendance annuelle", "Nombre total de crimes agrégés par année.")
    if not df_yearly.empty:
        yearly_plot = (
            df_yearly.groupby("year", as_index=False)["total_crimes"]
            .sum()
            .assign(period=lambda frame: pd.to_datetime(frame["year"].astype(int).astype(str) + "-01-01"))
            .sort_values("period")
        )
        st.altair_chart(
            line_chart(
                yearly_plot,
                "period",
                "total_crimes",
                color=NEUTRAL,
                height=300,
                x_title="Annee",
                y_title="Nombre de crimes",
            ),
            use_container_width=True,
        )
    else:
        st.info("Table annuelle non disponible.")

render_section_header("Territoires", "Top zones communautaires par volume de crimes.")
if not df_community.empty:
    community_plot = (
        df_community.sort_values("total_crimes", ascending=False)
        .head(12)
        .astype({"community_area": str})
    )
    st.altair_chart(
            bar_chart(
                community_plot,
                "total_crimes",
                "community_area",
                color=OK,
                horizontal=True,
                height=360,
                x_title="Nombre de crimes",
                y_title="Community area",
            ),
            use_container_width=True,
        )
    with st.expander("Voir les données agrégées", expanded=False):
        st.dataframe(df_community, use_container_width=True, hide_index=True)
else:
    st.info("Table communautaire non disponible.")
