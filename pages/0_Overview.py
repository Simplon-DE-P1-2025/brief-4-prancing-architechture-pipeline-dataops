import pandas as pd
import streamlit as st

from streamlit_dashboard.charts import ALERT, OK, bar_chart
from streamlit_dashboard.config import REPORTS_DIR, SODA_CONFIG_PATH
from streamlit_dashboard.metrics import (
    describe_freshness,
    format_date,
    format_number,
    format_percent,
    format_timestamp,
    safe_ratio,
)
from streamlit_dashboard.services.postgres import (
    get_db_config,
    get_db_connection_error,
    get_table_snapshot,
)
from streamlit_dashboard.services.reports import get_report_overview
from streamlit_dashboard.ui import (
    apply_app_style,
    render_detail_panel,
    render_metric_cards,
    render_page_header,
    render_section_header,
    render_sidebar,
    render_spacer,
    render_status_box,
    render_status_cards,
)


apply_app_style()
render_sidebar()
render_page_header(
    "Overview",
    "Vue de synthese du pipeline avec statuts de contrats, tables finales et contexte technique.",
    "Overview",
)

raw_overview = get_report_overview("raw")
processed_overview = get_report_overview("processed")
db_error = get_db_connection_error()
db_config = get_db_config()

valid_snapshot = get_table_snapshot(
    "chicago_crimes",
    freshness_column="updated_on",
    date_column="date",
)
quarantine_snapshot = get_table_snapshot(
    "chicago_crimes_quarantine",
    freshness_column="quarantined_at",
    date_column="date",
)

loaded_valid = valid_snapshot.get("row_count")
loaded_quarantine = quarantine_snapshot.get("row_count")
rejection_rate = safe_ratio(
    loaded_quarantine,
    (loaded_valid or 0) + (loaded_quarantine or 0),
)

if db_error:
    render_status_box(
        "warn",
        "PostgreSQL indisponible",
        "Les statuts qualité restent visibles, mais les métriques des tables finales sont partielles.",
    )

render_status_cards(
    [
        {
            "label": "Raw contract",
            "value": raw_overview["status_label"],
            "status": raw_overview["status"],
            "caption": f"Dernier scan {raw_overview['last_updated']}",
        },
        {
            "label": "Processed contract",
            "value": processed_overview["status_label"],
            "status": processed_overview["status"],
            "caption": f"Dernier scan {processed_overview['last_updated']}",
        },
        {
            "label": "Database",
            "value": "Disponible" if not db_error else "Partielle",
            "status": "ok" if not db_error else "warn",
            "caption": "Tables finales et aggregations",
        },
    ]
)

render_spacer("1.35rem")

render_metric_cards(
    [
        {
            "label": "Raw valid ratio",
            "value": format_percent(raw_overview["valid_ratio"]),
            "caption": f"{format_number(raw_overview['valid_rows'])} lignes valides",
            "tone": raw_overview["status"],
        },
        {
            "label": "Processed valid ratio",
            "value": format_percent(processed_overview["valid_ratio"]),
            "caption": f"{format_number(processed_overview['valid_rows'])} lignes valides",
            "tone": processed_overview["status"],
        },
        {
            "label": "Valid table",
            "value": format_number(loaded_valid),
            "caption": f"Fraicheur {describe_freshness(valid_snapshot.get('freshness_at'))}",
            "tone": "ok",
        },
        {
            "label": "Quarantine table",
            "value": format_number(loaded_quarantine),
            "caption": f"Taux de rejet {format_percent(rejection_rate)}",
            "tone": "warn" if (loaded_quarantine or 0) else "neutral",
        },
    ]
)

render_spacer("1.35rem")

render_metric_cards(
    [
        {
            "label": "Valid coverage",
            "value": format_date(valid_snapshot.get("min_date")),
            "caption": f"jusqu'au {format_date(valid_snapshot.get('max_date'))}",
            "tone": "ok",
        },
        {
            "label": "Valid freshness",
            "value": describe_freshness(valid_snapshot.get("freshness_at")),
            "caption": format_timestamp(valid_snapshot.get("freshness_at")),
            "tone": "ok",
        },
        {
            "label": "Quarantine freshness",
            "value": describe_freshness(quarantine_snapshot.get("freshness_at")),
            "caption": format_timestamp(quarantine_snapshot.get("freshness_at")),
            "tone": "warn",
        },
        {
            "label": "Quarantine coverage",
            "value": format_date(quarantine_snapshot.get("min_date")),
            "caption": f"jusqu'au {format_date(quarantine_snapshot.get('max_date'))}",
            "tone": "warn",
        },
    ]
)

render_spacer("1.6rem")

left_col, right_col = st.columns([1.15, 0.85], gap="large")

with left_col:
    render_section_header(
        "Funnel qualite",
        "Suivi du volume de lignes entre l'extraction, la validation et le chargement final.",
    )
    funnel_df = pd.DataFrame(
        [
            {"stage": "Raw total", "rows": raw_overview["total_rows"] or 0},
            {"stage": "Raw valid", "rows": raw_overview["valid_rows"] or 0},
            {"stage": "Processed total", "rows": processed_overview["total_rows"] or 0},
            {"stage": "Processed valid", "rows": processed_overview["valid_rows"] or 0},
            {"stage": "Loaded valid", "rows": loaded_valid or 0},
        ]
    )
    st.altair_chart(
        bar_chart(
            funnel_df,
            "stage",
            "rows",
            color=OK,
            sort=None,
            height=320,
            x_title="Etapes du pipeline",
            y_title="Nombre de lignes",
            x_label_angle=0,
        ),
        use_container_width=True,
    )

    render_spacer("1rem")
    render_section_header(
        "Alertes dominantes",
        "Les raisons les plus visibles dans les controles courants.",
    )
    combined_alerts = []
    for overview in [raw_overview, processed_overview]:
        reasons_df = overview["reasons"]
        if reasons_df.empty:
            continue
        top_reason = reasons_df.sort_values("row_count", ascending=False).iloc[0]
        combined_alerts.append(
            {
                "scope": overview["report_key"],
                "reason": top_reason["quarantine_reason"],
                "rows": top_reason["row_count"],
            }
        )

    if combined_alerts:
        alerts_df = pd.DataFrame(combined_alerts)
        st.altair_chart(
            bar_chart(
                alerts_df,
                "rows",
                "reason",
                color=ALERT,
                horizontal=True,
                height=220,
                x_title="Nombre de lignes",
                y_title="Raison",
            ),
            use_container_width=True,
        )
    else:
        st.info("Aucune alerte dominante detectee.")

with right_col:
    render_section_header(
        "Contexte",
        "Informations techniques utiles pour lire l'etat courant du dashboard.",
    )
    render_detail_panel(
        "Rapports",
        [
            ("Chemin", str(REPORTS_DIR)),
            ("Soda config", "detectee" if SODA_CONFIG_PATH.exists() else "absente"),
        ],
    )
    render_spacer("0.8rem")
    render_detail_panel(
        "PostgreSQL",
        [
            ("Host", str(db_config.get("host", "-"))),
            ("Base", str(db_config.get("database", "-"))),
            ("User", str(db_config.get("user", "-"))),
            ("Connexion", "OK" if not db_error else "indisponible"),
        ],
    )
