import pandas as pd
import streamlit as st


def apply_app_style():
    st.markdown(
        """
        <style>
        :root {
            --ink: #16262E;
            --muted: #52636B;
            --line: #C8B89E;
            --paper: #D8C5A6;
            --surface: #E8D9C2;
            --panel: #F6EEDF;
            --ok: #2E8B57;
            --warn: #C97A1A;
            --alert: #C44536;
            --accent: #1D5C63;
            --accent-soft: #D6E7E2;
            --sidebar: #BFA988;
            --sidebar-ink: #132127;
        }
        html, body, [class*="css"]  {
            color: var(--ink);
        }
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(29, 92, 99, 0.12), transparent 24%),
                radial-gradient(circle at top left, rgba(201, 122, 26, 0.12), transparent 22%),
                linear-gradient(180deg, #DECBAE 0%, #D2BD9E 100%);
            color: var(--ink);
        }
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(29, 92, 99, 0.12), transparent 24%),
                radial-gradient(circle at top left, rgba(201, 122, 26, 0.12), transparent 22%),
                linear-gradient(180deg, #DECBAE 0%, #D2BD9E 100%);
        }
        section.main > div {
            background: transparent !important;
        }
        [data-testid="stHeader"] {
            background: rgba(216, 197, 166, 0.82);
        }
        .block-container {
            padding-top: 1.3rem;
            padding-bottom: 2.5rem;
            max-width: 1280px;
        }
        .hero {
            padding: 1.45rem 1.55rem;
            border-radius: 24px;
            background: linear-gradient(135deg, #E2C59D 0%, #C9DDD6 100%);
            border: 1px solid rgba(22, 38, 46, 0.14);
            box-shadow: 0 16px 38px rgba(22, 38, 46, 0.10);
            margin-bottom: 1rem;
        }
        .hero-badge {
            display: inline-block;
            padding: 0.25rem 0.65rem;
            border-radius: 999px;
            background: rgba(22, 38, 46, 0.12);
            color: #102027;
            font-size: 0.72rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.7rem;
            font-weight: 700;
        }
        .hero h1 {
            margin: 0;
            color: var(--ink);
            font-size: 2.15rem;
            line-height: 1.05;
        }
        .hero p {
            margin: 0.5rem 0 0;
            color: #35515B;
            max-width: 820px;
        }
        .section-title {
            margin-top: 0.4rem;
            margin-bottom: 0.15rem;
            color: var(--ink);
            font-size: 1.05rem;
            font-weight: 700;
        }
        .section-caption {
            color: var(--muted);
            margin-bottom: 0.85rem;
            font-size: 0.92rem;
        }
        .panel-note {
            padding: 0.95rem 1rem;
            border-radius: 18px;
            border: 1px solid var(--line);
            background: rgba(246, 238, 223, 0.92);
            margin-bottom: 1rem;
        }
        .metric-card {
            border: 1px solid rgba(22, 38, 46, 0.14);
            border-top: 4px solid var(--accent);
            border-radius: 20px;
            padding: 1rem 1rem 1.1rem;
            background: rgba(246, 238, 223, 0.98);
            box-shadow: 0 14px 32px rgba(22, 38, 46, 0.10);
            min-height: 132px;
        }
        .metric-card.tone-ok { border-top-color: var(--ok); }
        .metric-card.tone-warn { border-top-color: var(--warn); }
        .metric-card.tone-alert { border-top-color: var(--alert); }
        .metric-card.tone-info { border-top-color: var(--accent); }
        .metric-card.tone-neutral { border-top-color: var(--ink); }
        .metric-label {
            font-size: 0.82rem;
            color: var(--muted);
            margin-bottom: 0.45rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .metric-value {
            font-size: 1.95rem;
            line-height: 1.1;
            font-weight: 700;
            color: var(--ink);
        }
        .metric-caption {
            margin-top: 0.7rem;
            color: #667E87;
            font-size: 0.86rem;
        }
        .status-card {
            border-radius: 18px;
            padding: 1.15rem 1rem 1.1rem;
            border: 1px solid rgba(22, 38, 46, 0.14);
            background: rgba(246, 238, 223, 0.98);
            min-height: 116px;
        }
        .status-dot {
            display: inline-block;
            width: 9px;
            height: 9px;
            border-radius: 999px;
            margin-right: 0.45rem;
            transform: translateY(-1px);
        }
        .dot-ok { background: var(--ok); }
        .dot-warn { background: var(--warn); }
        .dot-info { background: var(--accent); }
        .status-title {
            color: var(--muted);
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .status-value {
            color: var(--ink);
            font-weight: 700;
            font-size: 1.2rem;
            margin-top: 0.7rem;
        }
        .status-caption {
            color: #667E87;
            font-size: 0.86rem;
            margin-top: 0.7rem;
        }
        .detail-panel {
            border: 1px solid rgba(22, 38, 46, 0.12);
            border-radius: 18px;
            background: rgba(246, 238, 223, 0.96);
            padding: 1rem 1rem 0.9rem;
            box-shadow: 0 12px 28px rgba(22, 38, 46, 0.08);
        }
        .detail-title {
            font-size: 0.88rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--muted);
            margin-bottom: 0.65rem;
            font-weight: 700;
        }
        .detail-row {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
            padding: 0.45rem 0;
            border-top: 1px solid rgba(22, 38, 46, 0.08);
        }
        .detail-row:first-of-type {
            border-top: none;
            padding-top: 0;
        }
        .detail-key {
            color: var(--muted);
            font-size: 0.92rem;
        }
        .detail-value {
            color: var(--ink);
            font-weight: 600;
            font-size: 0.92rem;
            text-align: right;
        }
        .status-box {
            padding: 0.95rem 1rem;
            border-radius: 18px;
            margin-bottom: 1rem;
            border: 1px solid transparent;
            box-shadow: 0 10px 24px rgba(22, 38, 46, 0.08);
        }
        .status-box.ok { background: #EDF8F0; border-color: #C5E6CE; color: #25543A; }
        .status-box.warn { background: #FFF4E6; border-color: #F1CF9F; color: #7E4D12; }
        .status-box.info { background: #EEF5F8; border-color: #CFE0E7; color: #24505D; }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #BDA381 0%, #B29773 100%);
            border-right: 1px solid rgba(22, 38, 46, 0.14);
        }
        [data-testid="stSidebar"] * {
            color: var(--sidebar-ink) !important;
        }
        [data-testid="stSidebarNav"] {
            margin-top: 0.4rem;
            margin-bottom: 1rem;
        }
        [data-testid="stSidebarNav"] ul {
            gap: 0.25rem;
        }
        [data-testid="stSidebarNav"] a {
            background: rgba(246, 238, 223, 0.38);
            border: 1px solid rgba(22, 38, 46, 0.10);
            border-radius: 14px;
            margin-bottom: 0.3rem;
        }
        [data-testid="stSidebarNav"] a:hover {
            background: rgba(246, 238, 223, 0.65);
            border-color: rgba(22, 38, 46, 0.18);
        }
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: #111111 !important;
            border-color: #111111 !important;
            box-shadow: 0 10px 20px rgba(17, 17, 17, 0.18);
        }
        [data-testid="stSidebarNav"] a[aria-current="page"] * {
            color: #F6EEDF !important;
            fill: #F6EEDF !important;
        }
        [data-testid="stSidebar"] .stButton button {
            background: rgba(246, 238, 223, 0.88);
            color: var(--ink) !important;
            border: 1px solid rgba(22, 38, 46, 0.18);
        }
        [data-testid="stDataFrame"] {
            border-radius: 16px;
            overflow: hidden;
        }
        .stButton button {
            background: #F2E7D4;
            color: var(--ink);
            border: 1px solid rgba(22, 38, 46, 0.18);
            border-radius: 12px;
        }
        .stButton button:hover {
            border-color: rgba(29, 92, 99, 0.35);
            color: var(--ink);
        }
        [data-baseweb="tab-list"] {
            gap: 0.4rem;
        }
        [data-baseweb="tab"] {
            background: rgba(246, 238, 223, 0.72);
            border: 1px solid rgba(22, 38, 46, 0.10);
            border-radius: 12px 12px 0 0;
            color: var(--ink) !important;
        }
        [aria-selected="true"][data-baseweb="tab"] {
            background: rgba(246, 238, 223, 0.98);
            border-bottom-color: transparent;
        }
        .stMarkdown,
        .stMarkdown p,
        .stMarkdown li,
        .stMarkdown span,
        h1, h2, h3, h4,
        .stCaption,
        .stText,
        label,
        .stSelectbox label,
        .stMultiSelect label,
        .stTextInput label,
        .stSlider label,
        .stAlert,
        .stInfo,
        .stWarning,
        .stSuccess {
            color: var(--ink) !important;
        }
        code {
            background: rgba(246, 238, 223, 0.92) !important;
            color: var(--ink) !important;
            border: 1px solid rgba(22, 38, 46, 0.12);
            border-radius: 8px;
            padding: 0.1rem 0.35rem;
        }
        pre, .stCodeBlock, [data-testid="stCodeBlock"] {
            background: rgba(246, 238, 223, 0.98) !important;
            color: var(--ink) !important;
            border: 1px solid rgba(22, 38, 46, 0.12);
            border-radius: 16px;
        }
        pre code, .stCodeBlock code, [data-testid="stCodeBlock"] code {
            background: transparent !important;
            border: none !important;
            color: var(--ink) !important;
        }
        [data-testid="stMetricValue"],
        [data-testid="stMetricLabel"] {
            color: var(--ink) !important;
        }
        .stTextInput input,
        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div,
        .stNumberInput input {
            background: rgba(246, 238, 223, 0.98) !important;
            color: var(--ink) !important;
            border-color: rgba(22, 38, 46, 0.18) !important;
        }
        .stDataFrame, .stTable {
            background: rgba(246, 238, 223, 0.98) !important;
        }
        [data-testid="stVerticalBlock"] > div:has(> .element-container [data-testid="stAltairChart"]),
        [data-testid="stVerticalBlock"] > div:has(> .element-container [data-testid="stDataFrame"]),
        [data-testid="stVerticalBlock"] > div:has(> .element-container [data-testid="stTable"]) {
            background: rgba(246, 238, 223, 0.72) !important;
            border: 1px solid rgba(22, 38, 46, 0.10);
            border-radius: 18px;
            padding: 0.75rem;
        }
        [data-testid="stAltairChart"],
        [data-testid="stArrowVegaLiteChart"],
        [data-testid="stDataFrame"],
        [data-testid="stTable"] {
            background: #F6EEDF !important;
            border-radius: 16px;
        }
        [data-testid="stAltairChart"] canvas,
        [data-testid="stArrowVegaLiteChart"] canvas,
        [data-testid="stAltairChart"] svg,
        [data-testid="stArrowVegaLiteChart"] svg {
            background: #F6EEDF !important;
        }
        [data-testid="stExpander"] {
            background: rgba(246, 238, 223, 0.86);
            border: 1px solid rgba(22, 38, 46, 0.12);
            border-radius: 16px;
        }
        [data-testid="stAlertContainer"] {
            color: var(--ink);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar():
    if st.sidebar.button("Rafraichir", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()


def render_page_header(title: str, caption: str, badge: str):
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-badge">{badge}</div>
            <h1>{title}</h1>
            <p>{caption}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status_box(kind: str, title: str, message: str):
    css_class = kind if kind in {"ok", "warn", "info"} else "info"
    st.markdown(
        f"""
        <div class="status-box {css_class}">
            <strong>{title}</strong><br/>
            {message}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header(title: str, caption: str | None = None):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if caption:
        st.markdown(
            f'<div class="section-caption">{caption}</div>',
            unsafe_allow_html=True,
        )


def render_metric_cards(items: list[dict[str, str | int]], columns: int | None = None):
    if not items:
        return
    cols = st.columns(columns or len(items))
    for col, item in zip(cols, items):
        tone = item.get("tone", "neutral")
        col.markdown(
            f"""
            <div class="metric-card tone-{tone}">
                <div class="metric-label">{item["label"]}</div>
                <div class="metric-value">{item["value"]}</div>
                <div class="metric-caption">{item.get("caption", "")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_status_cards(items: list[dict[str, str]], columns: int | None = None):
    if not items:
        return
    cols = st.columns(columns or len(items))
    for col, item in zip(cols, items):
        status = item.get("status", "info")
        dot_class = {"ok": "dot-ok", "warn": "dot-warn", "info": "dot-info"}.get(
            status, "dot-info"
        )
        col.markdown(
            f"""
            <div class="status-card">
                <div class="status-title">
                    <span class="status-dot {dot_class}"></span>{item["label"]}
                </div>
                <div class="status-value">{item["value"]}</div>
                <div class="status-caption">{item.get("caption", "")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_detail_panel(title: str, items: list[tuple[str, str]]):
    rows_html = "".join(
        f"""
        <div class="detail-row">
            <div class="detail-key">{key}</div>
            <div class="detail-value">{value}</div>
        </div>
        """
        for key, value in items
    )
    st.markdown(
        f"""
        <div class="detail-panel">
            <div class="detail-title">{title}</div>
            {rows_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_spacer(height: str = "1rem"):
    st.markdown(
        f'<div style="height:{height};"></div>',
        unsafe_allow_html=True,
    )


def render_dataframe_block(
    title: str,
    df: pd.DataFrame,
    empty_message: str,
    download_name: str,
):
    render_section_header(title)
    if df.empty:
        st.info(empty_message)
        return

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button(
        label=f"Telecharger {title}",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=download_name,
        mime="text/csv",
        key=f"download_{download_name}",
    )
