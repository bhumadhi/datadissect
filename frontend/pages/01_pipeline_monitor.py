import os
import pandas as pd
import streamlit as st
from trino.dbapi import connect

st.set_page_config(
    page_title="Pipeline Monitor",
    page_icon="📡",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

.page-header {
    margin-bottom: 32px;
    padding-bottom: 24px;
    border-bottom: 1px solid #2a2a3a;
}

.page-label {
    font-family: 'DM Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.2em;
    color: #00e5a0;
    text-transform: uppercase;
    margin-bottom: 8px;
}

.page-title {
    font-family: 'Syne', sans-serif;
    font-size: 32px;
    font-weight: 800;
    color: #e8e8f0;
}

.metric-card {
    background: #111118;
    border: 1px solid #2a2a3a;
    border-radius: 12px;
    padding: 20px 24px;
}

.metric-label {
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #7070a0;
    margin-bottom: 8px;
}

.metric-value {
    font-family: 'Syne', sans-serif;
    font-size: 32px;
    font-weight: 800;
    color: #e8e8f0;
}

.metric-sub { font-size: 12px; color: #7070a0; margin-top: 4px; }

.status-badge {
    display: inline-block;
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 3px 10px;
    border-radius: 4px;
    font-weight: 500;
}

.section-title {
    font-family: 'Syne', sans-serif;
    font-size: 18px;
    font-weight: 700;
    color: #e8e8f0;
    margin: 32px 0 16px;
}
</style>
""", unsafe_allow_html=True)


# ── Trino Connection ──────────────────────────────────────────
@st.cache_resource
def get_trino_conn():
    return connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8083")),
        user="streamlit",
        catalog="postgresql",
        schema="public",
    )


@st.cache_data(ttl=30)
def query(sql: str) -> pd.DataFrame:
    conn = get_trino_conn()
    return pd.read_sql(sql, conn)


# ── Header ────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
    <div class="page-label">Operations</div>
    <div class="page-title">Pipeline Monitor</div>
</div>
""", unsafe_allow_html=True)

# Refresh button
if st.button("⟳ Refresh", type="secondary"):
    st.cache_data.clear()
    st.rerun()

# ── KPI Metrics ───────────────────────────────────────────────
try:
    kpi_df = query("""
        SELECT
            COUNT(*)                                            AS total_files,
            COUNT(*) FILTER (WHERE ingestion_status = 'CURATED')    AS curated,
            COUNT(*) FILTER (WHERE ingestion_status = 'FAILED')     AS failed,
            COUNT(*) FILTER (WHERE ingestion_status = 'QUARANTINED') AS quarantined,
            COUNT(DISTINCT client_code)                         AS unique_clients
        FROM postgresql.public.file_registry
    """)

    runs_df = query("""
        SELECT
            COUNT(*)                                            AS total_runs,
            COUNT(*) FILTER (WHERE run_status = 'SUCCESS')     AS successful,
            COUNT(*) FILTER (WHERE run_status = 'FAILED')      AS failed,
            ROUND(AVG(date_diff('second', started_at, ended_at)), 1) AS avg_duration_secs
        FROM postgresql.public.pipeline_run
        WHERE ended_at IS NOT NULL
    """)

    col1, col2, col3, col4, col5 = st.columns(5)

    metrics = [
        (col1, "Total Files",     kpi_df['total_files'][0],    "received"),
        (col2, "Curated",         kpi_df['curated'][0],        "fully processed"),
        (col3, "Failed",          kpi_df['failed'][0],         "need attention"),
        (col4, "Unique Clients",  kpi_df['unique_clients'][0], "sources"),
        (col5, "Avg Duration",    f"{runs_df['avg_duration_secs'][0]}s", "per pipeline run"),
    ]

    for col, label, value, sub in metrics:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
                <div class="metric-sub">{sub}</div>
            </div>
            """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"Could not load metrics: {e}")

# ── File Registry ─────────────────────────────────────────────
st.markdown('<div class="section-title">File Registry</div>', unsafe_allow_html=True)

try:
    registry_df = query("""
        SELECT
            file_name,
            client_code,
            file_type,
            env,
            sequence,
            ingestion_status,
            file_size_bytes,
            received_at,
            processed_at,
            error_message
        FROM postgresql.public.file_registry
        ORDER BY received_at DESC
        LIMIT 50
    """)

    # Color-code status
    def color_status(val):
        colors = {
            'CURATED':     'background-color: rgba(0,229,160,0.15); color: #00e5a0',
            'TRANSFORMED': 'background-color: rgba(124,106,247,0.15); color: #7c6af7',
            'CLEANSED':    'background-color: rgba(247,162,106,0.15); color: #f7a26a',
            'FAILED':      'background-color: rgba(255,107,107,0.15); color: #ff6b6b',
            'QUARANTINED': 'background-color: rgba(255,107,107,0.15); color: #ff6b6b',
            'RECEIVED':    'background-color: rgba(112,112,160,0.15); color: #7070a0',
        }
        return colors.get(val, '')

    styled = registry_df.style.applymap(color_status, subset=['ingestion_status'])
    st.dataframe(styled, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load file registry: {e}")

# ── Pipeline Run History ──────────────────────────────────────
st.markdown('<div class="section-title">Pipeline Run History</div>', unsafe_allow_html=True)

try:
    runs_history_df = query("""
        SELECT
            pipeline_name,
            run_status,
            records_read,
            records_written,
            records_rejected,
            date_diff('second', started_at, ended_at) AS duration_secs,
            started_at,
            error_message
        FROM postgresql.public.pipeline_run
        ORDER BY started_at DESC
        LIMIT 50
    """)

    def color_run_status(val):
        colors = {
            'SUCCESS': 'background-color: rgba(0,229,160,0.15); color: #00e5a0',
            'FAILED':  'background-color: rgba(255,107,107,0.15); color: #ff6b6b',
            'RUNNING': 'background-color: rgba(247,162,106,0.15); color: #f7a26a',
            'PARTIAL': 'background-color: rgba(124,106,247,0.15); color: #7c6af7',
        }
        return colors.get(val, '')

    styled_runs = runs_history_df.style.applymap(color_run_status, subset=['run_status'])
    st.dataframe(styled_runs, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load pipeline runs: {e}")

# ── Cross-source query: file + pipeline joined ────────────────
st.markdown('<div class="section-title">File → Pipeline Lineage</div>', unsafe_allow_html=True)

try:
    lineage_df = query("""
        SELECT
            f.file_name,
            f.client_code,
            f.env,
            f.ingestion_status,
            p.pipeline_name,
            p.records_read,
            p.records_written,
            p.run_status,
            date_diff('second', p.started_at, p.ended_at) AS duration_secs
        FROM postgresql.public.file_registry f
        JOIN postgresql.public.pipeline_run p
            ON p.pipeline_name = 'claims_curate_job'
        ORDER BY f.received_at DESC
        LIMIT 20
    """)
    st.dataframe(lineage_df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load lineage: {e}")