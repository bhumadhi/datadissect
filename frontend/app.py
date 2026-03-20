import streamlit as st

st.set_page_config(
    page_title="Healthcare Data Platform",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

.main { background-color: #0a0a0f; }

.hero {
    background: linear-gradient(135deg, #0a0a0f 0%, #111118 50%, #0a0a0f 100%);
    border: 1px solid #2a2a3a;
    border-radius: 16px;
    padding: 48px;
    margin-bottom: 32px;
    position: relative;
    overflow: hidden;
}

.hero::before {
    content: '';
    position: absolute;
    top: -50%;
    right: -20%;
    width: 500px;
    height: 500px;
    background: radial-gradient(circle, rgba(0,229,160,0.05) 0%, transparent 70%);
    pointer-events: none;
}

.hero-label {
    font-family: 'DM Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.2em;
    color: #00e5a0;
    text-transform: uppercase;
    margin-bottom: 16px;
}

.hero-title {
    font-family: 'Syne', sans-serif;
    font-size: 48px;
    font-weight: 800;
    color: #e8e8f0;
    line-height: 1.1;
    margin-bottom: 16px;
}

.hero-title span { color: #00e5a0; }

.hero-sub {
    font-size: 16px;
    color: #7070a0;
    font-weight: 300;
    max-width: 600px;
    line-height: 1.7;
}

.stack-card {
    background: #111118;
    border: 1px solid #2a2a3a;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 12px;
    transition: border-color 0.2s;
}

.stack-card:hover { border-color: #00e5a0; }

.stack-label {
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #7070a0;
    margin-bottom: 6px;
}

.stack-value {
    font-size: 15px;
    color: #e8e8f0;
    font-weight: 400;
}

.flow-container {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    margin: 24px 0;
}

.flow-step {
    background: #111118;
    border: 1px solid #2a2a3a;
    border-radius: 8px;
    padding: 8px 16px;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #00e5a0;
}

.flow-arrow {
    color: #2a2a3a;
    font-size: 18px;
}

.nav-card {
    background: #111118;
    border: 1px solid #2a2a3a;
    border-radius: 12px;
    padding: 24px;
    text-align: center;
    cursor: pointer;
    transition: all 0.2s;
}

.nav-card:hover {
    border-color: #00e5a0;
    background: #1a1a24;
}

.nav-icon { font-size: 32px; margin-bottom: 12px; }

.nav-title {
    font-family: 'Syne', sans-serif;
    font-size: 16px;
    font-weight: 700;
    color: #e8e8f0;
    margin-bottom: 8px;
}

.nav-desc { font-size: 13px; color: #7070a0; line-height: 1.5; }
</style>
""", unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="hero-label">Healthcare Data Platform</div>
    <div class="hero-title">Claims Pipeline<br><span>Observatory</span></div>
    <div class="hero-sub">
        End-to-end healthcare claims processing — from raw EDI files
        to curated reporting-ready Delta tables, queryable via Trino.
    </div>
    <div class="flow-container">
        <div class="flow-step">File Drop</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">Watcher</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">MinIO</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">Airflow</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">Spark</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">Delta Lake</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">Trino</div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">Streamlit</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Navigation ────────────────────────────────────────────────
st.markdown("### Navigate")
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="nav-card">
        <div class="nav-icon">📡</div>
        <div class="nav-title">Pipeline Monitor</div>
        <div class="nav-desc">
            File registry status, pipeline run history,
            record counts, failure tracking, quarantine audit.
        </div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="nav-card">
        <div class="nav-icon">📊</div>
        <div class="nav-title">Claims Analytics</div>
        <div class="nav-desc">
            Member, payer, and provider summaries.
            Billed amount distributions, chronic condition flags.
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Stack ─────────────────────────────────────────────────────
st.markdown("### Tech Stack")

stack = [
    ("Ingestion",       "File Watcher (watchdog) + Airflow REST API trigger"),
    ("Storage",         "MinIO — S3-compatible object storage (local cloud simulation)"),
    ("Processing",      "Apache Spark 3.5.1 + Delta Lake 3.0.0 (PySpark)"),
    ("Orchestration",   "Apache Airflow 2.9.1 — 7-task DAG with fail-fast validation"),
    ("Query Engine",    "Trino 435 — Delta + PostgreSQL connectors"),
    ("Metadata",        "PostgreSQL 16 — file_registry + pipeline_run audit tables"),
    ("Frontend",        "Streamlit — operational monitor + claims analytics"),
]

col1, col2 = st.columns(2)
for i, (label, value) in enumerate(stack):
    with (col1 if i % 2 == 0 else col2):
        st.markdown(f"""
        <div class="stack-card">
            <div class="stack-label">{label}</div>
            <div class="stack-value">{value}</div>
        </div>
        """, unsafe_allow_html=True)