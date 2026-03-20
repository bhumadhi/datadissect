import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from trino.dbapi import connect

st.set_page_config(
    page_title="Claims Analytics",
    page_icon="📊",
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
    color: #7c6af7;
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

.section-title {
    font-family: 'Syne', sans-serif;
    font-size: 18px;
    font-weight: 700;
    color: #e8e8f0;
    margin: 32px 0 16px;
}
</style>
""", unsafe_allow_html=True)

# Plotly dark theme
PLOT_THEME = dict(
    plot_bgcolor='#111118',
    paper_bgcolor='#111118',
    font=dict(color='#e8e8f0', family='DM Sans'),
    xaxis=dict(gridcolor='#2a2a3a', linecolor='#2a2a3a'),
    yaxis=dict(gridcolor='#2a2a3a', linecolor='#2a2a3a'),
)

COLORS = ['#00e5a0', '#7c6af7', '#f7a26a', '#ff6b6b', '#4ecdc4', '#ffe66d']


# ── Trino Connection ──────────────────────────────────────────
@st.cache_resource
def get_trino_conn():
    return connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8083")),
        user="streamlit",
        catalog="delta",
        schema="healthcare",
    )


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    conn = get_trino_conn()
    return pd.read_sql(sql, conn)


# ── Header ────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
    <div class="page-label">Analytics</div>
    <div class="page-title">Claims Analytics</div>
</div>
""", unsafe_allow_html=True)

if st.button("⟳ Refresh", type="secondary"):
    st.cache_data.clear()
    st.rerun()

# ── KPI Metrics ───────────────────────────────────────────────
try:
    member_kpi = query("""
        SELECT
            COUNT(*)                             AS total_members,
            SUM(total_claims)                    AS total_claims,
            ROUND(SUM(total_billed), 2)          AS total_billed,
            ROUND(AVG(avg_billed), 2)            AS avg_billed_per_member,
            COUNT_IF(has_chronic_condition)      AS chronic_members
        FROM delta.healthcare.member_summary
    """)

    col1, col2, col3, col4, col5 = st.columns(5)

    metrics = [
        (col1, "Total Members",      member_kpi['total_members'][0],          "unique"),
        (col2, "Total Claims",       f"{int(member_kpi['total_claims'][0]):,}", "processed"),
        (col3, "Total Billed",       f"${member_kpi['total_billed'][0]:,.2f}", "USD"),
        (col4, "Avg Billed/Member",  f"${member_kpi['avg_billed_per_member'][0]:,.2f}", "per member"),
        (col5, "Chronic Members",    member_kpi['chronic_members'][0],         "with chronic conditions"),
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
    st.error(f"Could not load member metrics: {e}")

# ── Payer Analysis ────────────────────────────────────────────
st.markdown('<div class="section-title">Payer Analysis</div>', unsafe_allow_html=True)

try:
    payer_df = query("""
        SELECT
            payer_id,
            total_claims,
            total_billed,
            avg_billed,
            unique_members,
            avg_claim_age_days,
            low_claims,
            medium_claims,
            high_claims
        FROM delta.healthcare.payer_summary
        ORDER BY total_billed DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            payer_df,
            x='payer_id',
            y='total_billed',
            title='Total Billed by Payer',
            color_discrete_sequence=[COLORS[0]],
        )
        fig.update_layout(**PLOT_THEME, title_font_family='Syne')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Stacked bar: LOW/MEDIUM/HIGH per payer
        fig2 = go.Figure()
        for i, (cat, color) in enumerate([
            ('low_claims', COLORS[0]),
            ('medium_claims', COLORS[2]),
            ('high_claims', COLORS[3]),
        ]):
            fig2.add_trace(go.Bar(
                name=cat.replace('_claims', '').upper(),
                x=payer_df['payer_id'],
                y=payer_df[cat],
                marker_color=color,
            ))
        fig2.update_layout(
            **PLOT_THEME,
            barmode='stack',
            title='Claim Volume by Category',
            title_font_family='Syne',
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(payer_df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load payer data: {e}")

# ── Provider Analysis ─────────────────────────────────────────
st.markdown('<div class="section-title">Provider Analysis</div>', unsafe_allow_html=True)

try:
    provider_df = query("""
        SELECT
            provider_npi_hash,
            total_claims,
            total_billed,
            avg_billed,
            unique_members,
            unique_payers,
            unique_cpt_codes
        FROM delta.healthcare.provider_summary
        ORDER BY total_billed DESC
        LIMIT 20
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig3 = px.bar(
            provider_df.head(10),
            x='total_billed',
            y='provider_npi_hash',
            orientation='h',
            title='Top 10 Providers by Billed Amount',
            color_discrete_sequence=[COLORS[1]],
        )
        fig3.update_layout(**PLOT_THEME, title_font_family='Syne')
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        fig4 = px.scatter(
            provider_df,
            x='total_claims',
            y='total_billed',
            size='unique_members',
            hover_data=['provider_npi_hash', 'unique_cpt_codes'],
            title='Claims vs Billed (bubble = unique members)',
            color_discrete_sequence=[COLORS[2]],
        )
        fig4.update_layout(**PLOT_THEME, title_font_family='Syne')
        st.plotly_chart(fig4, use_container_width=True)

    st.dataframe(provider_df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load provider data: {e}")

# ── Member Analysis ───────────────────────────────────────────
st.markdown('<div class="section-title">Member Analysis</div>', unsafe_allow_html=True)

try:
    member_df = query("""
        SELECT
            member_id_hash,
            total_claims,
            total_billed,
            avg_billed,
            unique_providers,
            unique_payers,
            has_chronic_condition,
            most_recent_service_date
        FROM delta.healthcare.member_summary
        ORDER BY total_billed DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        # Chronic vs non-chronic
        chronic_counts = member_df['has_chronic_condition'].value_counts().reset_index()
        chronic_counts.columns = ['has_chronic', 'count']
        chronic_counts['label'] = chronic_counts['has_chronic'].map(
            {True: 'Chronic', False: 'Non-Chronic'}
        )
        fig5 = px.pie(
            chronic_counts,
            values='count',
            names='label',
            title='Chronic vs Non-Chronic Members',
            color_discrete_sequence=[COLORS[3], COLORS[0]],
            hole=0.5,
        )
        fig5.update_layout(**PLOT_THEME, title_font_family='Syne')
        st.plotly_chart(fig5, use_container_width=True)

    with col2:
        # Total billed distribution
        fig6 = px.histogram(
            member_df,
            x='total_billed',
            nbins=20,
            title='Member Total Billed Distribution',
            color_discrete_sequence=[COLORS[0]],
        )
        fig6.update_layout(**PLOT_THEME, title_font_family='Syne')
        st.plotly_chart(fig6, use_container_width=True)

    st.dataframe(member_df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load member data: {e}")