"""Supply Chain Decision Engine — Streamlit dashboard."""

import os

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Supply Chain Decision Engine",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Global styles ──────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
    .metric-card {
        background: #f8f9fb;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        text-align: center;
    }
    .metric-label { font-size: 0.78rem; color: #64748b; font-weight: 600;
                    letter-spacing: 0.05em; text-transform: uppercase; margin-bottom: 4px; }
    .metric-value { font-size: 1.85rem; font-weight: 700; color: #1e293b; }
    .metric-value.warn { color: #d97706; }
    .metric-value.danger { color: #dc2626; }
    .section-header {
        font-size: 1.1rem; font-weight: 700; color: #1e293b;
        border-left: 4px solid #3b82f6; padding-left: 0.6rem;
        margin: 1.5rem 0 0.75rem;
    }
    .placeholder-box {
        background: #f1f5f9; border: 1px dashed #94a3b8;
        border-radius: 8px; padding: 1rem 1.25rem; color: #64748b;
        font-size: 0.9rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── DB helper ──────────────────────────────────────────────────────────────────

@st.cache_resource
def _conn():
    return duckdb.connect(DB_PATH, read_only=True)


def q(sql: str) -> pd.DataFrame:
    try:
        return _conn().execute(sql).fetchdf()
    except Exception as exc:
        st.error(f"Query error: {exc}")
        return pd.DataFrame()


# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown("## 🚚 Supply Chain Decision Engine")
st.caption("Real-time risk intelligence · Olist dataset · DuckDB gold layer")
st.divider()

# ── 1. Executive KPIs ──────────────────────────────────────────────────────────

st.markdown('<div class="section-header">Executive Summary</div>', unsafe_allow_html=True)

kpi = q("SELECT * FROM gold.gold_executive_summary")

if not kpi.empty:
    row = kpi.iloc[0]
    total_orders  = int(row["total_orders"])
    total_sellers = int(row["total_sellers"])
    late_rate     = float(row["overall_late_rate"])
    pct_high_risk = float(row["pct_high_risk_sellers"])
    avg_review    = float(row["avg_review_score"])

    c1, c2, c3, c4, c5 = st.columns(5)

    def _kpi(col, label, value, extra_class=""):
        col.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">{label}</div>'
            f'<div class="metric-value {extra_class}">{value}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    _kpi(c1, "Total Orders",      f"{total_orders:,}")
    _kpi(c2, "Total Sellers",     f"{total_sellers:,}")
    _kpi(c3, "Overall Late Rate", f"{late_rate:.1%}",
         "warn" if late_rate > 0.1 else "")
    _kpi(c4, "High-Risk Sellers", f"{pct_high_risk:.1%}",
         "danger" if pct_high_risk > 0.1 else "warn" if pct_high_risk > 0.05 else "")
    _kpi(c5, "Avg Review Score",  f"{avg_review:.2f} / 5",
         "danger" if avg_review < 3.5 else "warn" if avg_review < 4.0 else "")
else:
    st.warning("Could not load executive summary. Run `dbt run --select gold` first.")

st.divider()

# ── 2 & 3: Risk table + Concentration chart ────────────────────────────────────

left, right = st.columns([3, 2], gap="large")

# ── 2. Supplier Risk Table ─────────────────────────────────────────────────────

with left:
    st.markdown('<div class="section-header">Supplier Risk</div>', unsafe_allow_html=True)

    risk_df = q("""
        SELECT
            seller_id,
            seller_state,
            risk_tier,
            total_orders,
            ROUND(late_delivery_rate * 100, 1)  AS late_rate_pct,
            ROUND(avg_review_score, 2)           AS review_score,
            ROUND(avg_delivery_days, 1)          AS delivery_days
        FROM gold.gold_supplier_risk
        ORDER BY
            CASE risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            late_rate_pct DESC
    """)

    if not risk_df.empty:
        tier_counts = risk_df["risk_tier"].value_counts()
        b1, b2, b3, b4 = st.columns([2, 1, 1, 1])
        b1.selectbox(
            "Filter by tier",
            options=["All", "HIGH", "MEDIUM", "LOW"],
            key="tier_filter",
            label_visibility="collapsed",
        )
        b2.metric("🔴 HIGH",   int(tier_counts.get("HIGH",   0)))
        b3.metric("🟡 MEDIUM", int(tier_counts.get("MEDIUM", 0)))
        b4.metric("🟢 LOW",    int(tier_counts.get("LOW",    0)))

        selected = st.session_state.get("tier_filter", "All")
        if selected != "All":
            risk_df = risk_df[risk_df["risk_tier"] == selected]

        TIER_COLORS = {"HIGH": "#fee2e2", "MEDIUM": "#fef9c3", "LOW": "#dcfce7"}

        def _color_row(row):
            bg = TIER_COLORS.get(row["risk_tier"], "white")
            return [f"background-color: {bg}; color: #1e293b" for _ in row]

        styled = (
            risk_df.style
            .apply(_color_row, axis=1)
            .format({
                "late_rate_pct": "{:.1f}%",
                "review_score":  "{:.2f}",
                "delivery_days": "{:.1f}",
            })
        )

        st.dataframe(
            styled,
            use_container_width=True,
            height=460,
            column_config={
                "seller_id":     st.column_config.TextColumn("Seller ID",     width="medium"),
                "seller_state":  st.column_config.TextColumn("State",         width="small"),
                "risk_tier":     st.column_config.TextColumn("Risk Tier",     width="small"),
                "total_orders":  st.column_config.NumberColumn("Orders",      width="small"),
                "late_rate_pct": st.column_config.TextColumn("Late Rate",     width="small"),
                "review_score":  st.column_config.TextColumn("Review Score",  width="small"),
                "delivery_days": st.column_config.TextColumn("Avg Del. Days", width="small"),
            },
            hide_index=True,
        )
    else:
        st.info("No supplier risk data found.")

# ── 3. Concentration Risk Bar Chart ───────────────────────────────────────────

with right:
    st.markdown(
        '<div class="section-header">Concentration Risk by State</div>',
        unsafe_allow_html=True,
    )

    conc_df = q("""
        SELECT seller_state, total_sellers, total_orders,
               ROUND(pct_of_total_orders * 100, 2) AS pct,
               concentration_flag
        FROM gold.gold_concentration_risk
        ORDER BY pct DESC
    """)

    if not conc_df.empty:
        colors = [
            "#ef4444" if f == "HIGH" else "#60a5fa"
            for f in conc_df["concentration_flag"]
        ]

        fig = go.Figure(go.Bar(
            x=conc_df["seller_state"],
            y=conc_df["pct"],
            marker_color=colors,
            text=conc_df["pct"].apply(lambda v: f"{v:.1f}%"),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Order share: %{y:.1f}%<extra></extra>",
        ))

        fig.add_hline(
            y=20,
            line_dash="dot",
            line_color="#dc2626",
            annotation_text="HIGH threshold (20%)",
            annotation_position="top right",
            annotation_font_color="#dc2626",
            annotation_font_size=11,
        )

        fig.update_layout(
            xaxis_title="Seller State",
            yaxis_title="% of Total Orders",
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=20, b=40, l=40, r=20),
            height=290,
            font=dict(family="sans-serif", size=12),
            xaxis=dict(tickangle=-45, showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            showlegend=False,
        )

        st.plotly_chart(fig, use_container_width=True)

        high_states = conc_df[conc_df["concentration_flag"] == "HIGH"]["seller_state"].tolist()
        if high_states:
            st.error(
                f"⚠️ **Concentration risk** — "
                f"{', '.join(high_states)} account{'s' if len(high_states) == 1 else ''} "
                f"for >20% of all orders."
            )

        with st.expander("Full state breakdown"):
            st.dataframe(
                conc_df.rename(columns={
                    "seller_state":       "State",
                    "total_sellers":      "Sellers",
                    "total_orders":       "Orders",
                    "pct":                "Order Share %",
                    "concentration_flag": "Flag",
                }),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No concentration risk data found.")

st.divider()

# ── 4. AI Assistant placeholder ───────────────────────────────────────────────

st.markdown(
    '<div class="section-header">AI Decision Assistant</div>',
    unsafe_allow_html=True,
)

question = st.text_input(
    label="Ask a supply chain question",
    placeholder="e.g. Which sellers are highest risk? Where is our concentration risk?",
)

if question:
    st.markdown(
        '<div class="placeholder-box">'
        "🔑 <strong>LLM integration coming soon</strong> — "
        "add <code>ANTHROPIC_API_KEY</code> to your <code>.env</code> file to enable."
        "</div>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="placeholder-box">'
        "🔑 <strong>LLM integration coming soon</strong> — "
        "add <code>ANTHROPIC_API_KEY</code> to your <code>.env</code> file to enable."
        "</div>",
        unsafe_allow_html=True,
    )

# ── Footer ─────────────────────────────────────────────────────────────────────

st.markdown(
    "<br><center><small style='color:#94a3b8'>"
    "Supply Chain Decision Engine · DuckDB medallion lakehouse · gold layer"
    "</small></center>",
    unsafe_allow_html=True,
)
