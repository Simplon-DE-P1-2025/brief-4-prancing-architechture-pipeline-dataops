import pandas as pd
import streamlit as st

from streamlit_dashboard.config import REPORTS_DIR, SODA_CONFIG_PATH
from streamlit_dashboard.services.postgres import get_db_config, get_db_connection_error


def apply_app_style():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
            max-width: 1280px;
        }
        .hero {
            padding: 1.2rem 1.4rem;
            border-radius: 18px;
            background: linear-gradient(135deg, #f4efe6 0%, #e6f1f5 100%);
            border: 1px solid #d6e3e8;
            margin-bottom: 1rem;
        }
        .hero h1 {
            margin: 0;
            color: #17313b;
            font-size: 2rem;
        }
        .hero p {
            margin: 0.45rem 0 0;
            color: #34505a;
        }
        .metric-card {
            border: 1px solid #d9e0e4;
            border-radius: 16px;
            padding: 1rem;
            background: #ffffff;
            min-height: 120px;
        }
        .metric-label {
            font-size: 0.9rem;
            color: #5c7179;
            margin-bottom: 0.35rem;
        }
        .metric-value {
            font-size: 1.8rem;
            font-weight: 700;
            color: #132c34;
        }
        .metric-caption {
            margin-top: 0.35rem;
            color: #667f87;
            font-size: 0.86rem;
        }
        .status-ok, .status-warn, .status-info {
            padding: 0.85rem 1rem;
            border-radius: 14px;
            margin-bottom: 1rem;
            border: 1px solid transparent;
        }
        .status-ok {
            background: #edf8f0;
            border-color: #b8dfc0;
            color: #245336;
        }
        .status-warn {
            background: #fff5e9;
            border-color: #f3d0a0;
            color: #7c4b11;
        }
        .status-info {
            background: #eef5f8;
            border-color: #c9dde6;
            color: #234a58;
        }
        .section-note {
            color: #5d747d;
            margin-bottom: 0.75rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar():
    db_config = get_db_config()
    db_error = get_db_connection_error()

    if st.sidebar.button("Rafraichir les donnees", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    st.sidebar.header("Contexte")
    st.sidebar.write(f"Rapports: `{REPORTS_DIR}`")
    if SODA_CONFIG_PATH.exists():
        st.sidebar.success("configuration.yml detecte")
    else:
        st.sidebar.warning("configuration.yml absent")

    st.sidebar.header("PostgreSQL")
    st.sidebar.write(f"Host: `{db_config.get('host', '-')}`")
    st.sidebar.write(f"Port: `{db_config.get('port', '-')}`")
    st.sidebar.write(f"Base: `{db_config.get('database', '-')}`")
    st.sidebar.write(f"User: `{db_config.get('user', '-')}`")
    if db_error:
        st.sidebar.error("Connexion DB indisponible")
    else:
        st.sidebar.success("Connexion DB OK")


def render_page_header(title: str, caption: str):
    st.markdown(
        f"""
        <div class="hero">
            <h1>{title}</h1>
            <p>{caption}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status_box(kind: str, title: str, message: str):
    css_class = {
        "ok": "status-ok",
        "warn": "status-warn",
        "info": "status-info",
    }.get(kind, "status-info")
    st.markdown(
        f"""
        <div class="{css_class}">
            <strong>{title}</strong><br/>
            {message}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_cards(items: list[dict[str, str]]):
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        col.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{item["label"]}</div>
                <div class="metric-value">{item["value"]}</div>
                <div class="metric-caption">{item.get("caption", "")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_health_cards(items: list[dict[str, str]]):
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        status = item.get("status", "info")
        status_label = {
            "ok": "OK",
            "warn": "A surveiller",
            "info": "Info",
        }.get(status, "Info")
        col.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{item["label"]}</div>
                <div class="metric-value">{status_label}</div>
                <div class="metric-caption">{item.get("caption", "")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_dataframe_block(
    title: str,
    df: pd.DataFrame,
    empty_message: str,
    download_name: str,
):
    st.markdown(f"### {title}")
    if df.empty:
        st.info(empty_message)
        return

    st.dataframe(df, use_container_width=True)
    st.download_button(
        label=f"Telecharger {title}",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=download_name,
        mime="text/csv",
        key=f"download_{download_name}",
    )
