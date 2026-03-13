import streamlit as st

from streamlit_dashboard.charts import ALERT, OK, WARN, bar_chart
from streamlit_dashboard.metrics import (
    describe_freshness,
    format_date,
    format_number,
    format_percent,
    format_timestamp,
    safe_ratio,
)
from streamlit_dashboard.services.postgres import (
    get_category_breakdown,
    get_db_connection_error,
    get_quarantine_column_breakdown,
    get_quarantine_reason_breakdown,
    get_table_preview,
    get_table_snapshot,
)
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
    "Data Health",
    "Lecture conjointe de la table valide et de la quarantaine, avec des indicateurs simples et actionnables.",
    "Data Health",
)

db_error = get_db_connection_error()
if db_error:
    render_status_box(
        "warn",
        "PostgreSQL indisponible",
        "La page Data Health depend des tables finales chargees en base.",
    )
    st.code(db_error)
    st.stop()

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
top_valid_categories = get_category_breakdown("chicago_crimes", limit=8)
top_quarantine_reasons = get_quarantine_reason_breakdown(limit=8)
top_quarantine_columns = get_quarantine_column_breakdown(limit=8)

valid_rows = valid_snapshot.get("row_count")
quarantine_rows = quarantine_snapshot.get("row_count")
rejection_rate = safe_ratio(
    quarantine_rows,
    (valid_rows or 0) + (quarantine_rows or 0),
)

render_metric_cards(
    [
        {
            "label": "Valid rows",
            "value": format_number(valid_rows),
            "caption": f"Fraicheur {describe_freshness(valid_snapshot.get('freshness_at'))}",
            "tone": "ok",
        },
        {
            "label": "Quarantine rows",
            "value": format_number(quarantine_rows),
            "caption": f"Fraicheur {describe_freshness(quarantine_snapshot.get('freshness_at'))}",
            "tone": "warn",
        },
        {
            "label": "Rejection rate",
            "value": format_percent(rejection_rate),
            "caption": "Part des lignes en quarantaine dans la sortie finale",
            "tone": "alert" if (rejection_rate or 0) > 0.1 else "warn",
        },
        {
            "label": "Coverage end",
            "value": format_date(valid_snapshot.get("max_date")),
            "caption": f"Depuis {format_date(valid_snapshot.get('min_date'))}",
            "tone": "neutral",
        },
    ]
)

valid_col, quarantine_col = st.columns(2)

with valid_col:
    render_section_header(
        "Valid",
        "Volume utile, couverture temporelle et categories dominantes.",
    )
    render_metric_cards(
        [
            {
                "label": "Rows",
                "value": format_number(valid_rows),
                "caption": "Table `chicago_crimes`",
                "tone": "ok",
            },
            {
                "label": "Coverage",
                "value": format_date(valid_snapshot.get("min_date")),
                "caption": f"jusqu'au {format_date(valid_snapshot.get('max_date'))}",
                "tone": "neutral",
            },
            {
                "label": "Freshness",
                "value": describe_freshness(valid_snapshot.get("freshness_at")),
                "caption": format_timestamp(valid_snapshot.get("freshness_at")),
                "tone": "ok",
            },
        ],
        columns=3,
    )

    if not top_valid_categories.empty:
        st.altair_chart(
            bar_chart(
                top_valid_categories,
                "value",
                "category",
                color=OK,
                horizontal=True,
                height=320,
                x_title="Nombre de lignes",
                y_title="Categorie",
            ),
            use_container_width=True,
        )
    else:
        st.info("La table valide n'est pas encore disponible.")

    with st.expander("Apercu de la table valide", expanded=False):
        st.dataframe(
            get_table_preview("chicago_crimes", limit=25),
            use_container_width=True,
            hide_index=True,
        )

with quarantine_col:
    render_section_header(
        "Quarantine",
        "Top raisons, colonnes en defaut et apercu filtrable.",
    )
    render_metric_cards(
        [
            {
                "label": "Rows",
                "value": format_number(quarantine_rows),
                "caption": "Table `chicago_crimes_quarantine`",
                "tone": "warn",
            },
            {
                "label": "Latest quarantine",
                "value": describe_freshness(quarantine_snapshot.get("freshness_at")),
                "caption": format_timestamp(quarantine_snapshot.get("freshness_at")),
                "tone": "warn",
            },
            {
                "label": "Rejection rate",
                "value": format_percent(rejection_rate),
                "caption": "Sortie finale",
                "tone": "alert" if (rejection_rate or 0) > 0.1 else "warn",
            },
        ],
        columns=3,
    )

    if not top_quarantine_reasons.empty:
        st.altair_chart(
            bar_chart(
                top_quarantine_reasons,
                "row_count",
                "quarantine_reason",
                color=ALERT,
                horizontal=True,
                height=250,
                x_title="Nombre de lignes",
                y_title="Raison",
            ),
            use_container_width=True,
        )
    else:
        st.info("Aucune raison de quarantaine disponible.")

    if not top_quarantine_columns.empty:
        st.altair_chart(
            bar_chart(
                top_quarantine_columns,
                "row_count",
                "column_name",
                color=WARN,
                horizontal=True,
                height=230,
                x_title="Nombre de lignes",
                y_title="Colonne",
            ),
            use_container_width=True,
        )

    preview_limit = st.slider("Nombre de lignes d'aperçu", 20, 300, 80, 20)
    quarantine_df = get_table_preview("chicago_crimes_quarantine", limit=preview_limit)

    if not quarantine_df.empty:
        if "quarantine_reason" in quarantine_df.columns:
            reasons = sorted(quarantine_df["quarantine_reason"].dropna().unique().tolist())
            selected_reasons = st.multiselect("Filtrer par raison", reasons)
            if selected_reasons:
                quarantine_df = quarantine_df.loc[
                    quarantine_df["quarantine_reason"].isin(selected_reasons)
                ]

        search = st.text_input("Recherche texte")
        if search:
            mask = quarantine_df.astype(str).apply(
                lambda column: column.str.contains(search, case=False, na=False)
            )
            quarantine_df = quarantine_df.loc[mask.any(axis=1)]

    st.dataframe(quarantine_df, use_container_width=True, hide_index=True)
