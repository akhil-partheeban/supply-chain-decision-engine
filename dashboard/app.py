"""Supply Chain Decision Engine — Streamlit dashboard."""

import os

import duckdb
import pandas as pd
import plotly.graph_objects as go
import requests
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
    total_orders     = int(row["total_orders"])
    total_suppliers  = int(row["total_suppliers"])
    total_revenue    = float(row["total_revenue"])
    late_rate        = float(row["overall_late_rate"])
    avg_reliability  = float(row["avg_reliability_score"])
    pct_high_risk    = float(row["pct_high_risk_suppliers"])
    hhi_index        = float(row["hhi_index"])
    hhi_interp       = str(row["hhi_interpretation"]).replace("_", " ").title()

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    def _kpi(col, label, value, extra_class="", caption=None):
        col.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">{label}</div>'
            f'<div class="metric-value {extra_class}">{value}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if caption:
            col.caption(caption)

    _kpi(c1, "Total Orders",       f"{total_orders:,}")
    _kpi(c2, "Suppliers Scored",   f"{total_suppliers:,}")
    _kpi(c3, "Total Revenue",      f"${total_revenue:,.0f}")
    _kpi(c4, "Overall Late Rate",  f"{late_rate:.1%}",
         "warn" if late_rate > 0.1 else "")
    _kpi(c5, "Avg Reliability",    f"{avg_reliability:.0f} / 100",
         "danger" if avg_reliability < 70 else "warn" if avg_reliability < 85 else "",
         caption=f"{pct_high_risk:.1%} high-risk")
    _kpi(c6, "Supplier HHI",       f"{hhi_index:.0f}",
         "danger" if hhi_index >= 2500 else "warn" if hhi_index >= 1500 else "",
         caption=hhi_interp)
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
            reliability_score,
            total_orders,
            ROUND(late_delivery_rate * 100, 1)  AS late_rate_pct,
            ROUND(avg_review_score, 2)           AS review_score,
            ROUND(avg_delivery_days, 1)          AS delivery_days,
            delivery_days_stddev
        FROM gold.gold_supplier_scorecard
        ORDER BY
            CASE risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            reliability_score ASC
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
                "late_rate_pct":         "{:.1f}%",
                "review_score":          "{:.2f}",
                "delivery_days":         "{:.1f}",
                "reliability_score":     "{:.0f}",
                "delivery_days_stddev":  "{:.1f}",
            })
        )

        st.dataframe(
            styled,
            use_container_width=True,
            height=460,
            column_config={
                "seller_id":            st.column_config.TextColumn("Seller ID",       width="medium"),
                "seller_state":         st.column_config.TextColumn("State",           width="small"),
                "risk_tier":            st.column_config.TextColumn("Risk Tier",       width="small"),
                "reliability_score":    st.column_config.TextColumn("Reliability",     width="small"),
                "total_orders":         st.column_config.NumberColumn("Orders",        width="small"),
                "late_rate_pct":        st.column_config.TextColumn("Late Rate",       width="small"),
                "review_score":         st.column_config.TextColumn("Review Score",    width="small"),
                "delivery_days":        st.column_config.TextColumn("Avg Del. Days",   width="small"),
                "delivery_days_stddev": st.column_config.TextColumn("Lead-Time σ",     width="small"),
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
        SELECT seller_state, total_sellers, total_revenue,
               pct_of_total_revenue AS pct,
               concentration_flag
        FROM gold.gold_geo_concentration
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
            hovertemplate="<b>%{x}</b><br>Revenue share: %{y:.1f}%<extra></extra>",
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
            yaxis_title="% of Total Revenue",
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
                f"⚠️ **Geographic concentration** — "
                f"{', '.join(high_states)} account{'s' if len(high_states) == 1 else ''} "
                f"for >20% of total revenue."
            )

        with st.expander("Full state breakdown"):
            st.dataframe(
                conc_df.rename(columns={
                    "seller_state":       "State",
                    "total_sellers":      "Sellers",
                    "total_revenue":      "Revenue",
                    "pct":                "Revenue Share %",
                    "concentration_flag": "Flag",
                }),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No concentration risk data found.")

st.divider()

# ── 3b. Supplier concentration (HHI) + Sourcing cost drivers ──────────────────

left2, right2 = st.columns([2, 3], gap="large")

with left2:
    st.markdown(
        '<div class="section-header">Supplier Concentration (HHI)</div>',
        unsafe_allow_html=True,
    )

    top_suppliers = q("""
        SELECT seller_id, seller_state, total_revenue, revenue_share_pct,
               cumulative_share_pct, concentration_flag
        FROM gold.gold_concentration_risk
        ORDER BY revenue_share_pct DESC
        LIMIT 10
    """)

    if not top_suppliers.empty and not kpi.empty:
        st.caption(
            f"Portfolio HHI: **{hhi_index:.0f}** ({hhi_interp}) · "
            f"Top-5 suppliers hold **{float(kpi.iloc[0]['top5_supplier_revenue_share_pct']):.1f}%** of revenue"
        )
        st.dataframe(
            top_suppliers.rename(columns={
                "seller_id":            "Seller ID",
                "seller_state":         "State",
                "total_revenue":        "Revenue",
                "revenue_share_pct":    "Share %",
                "cumulative_share_pct": "Cumulative %",
                "concentration_flag":   "Flag",
            }),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Revenue": st.column_config.NumberColumn(format="$%.0f"),
                "Share %": st.column_config.NumberColumn(format="%.2f%%"),
                "Cumulative %": st.column_config.NumberColumn(format="%.1f%%"),
            },
        )
    else:
        st.info("No supplier concentration data found.")

with right2:
    st.markdown(
        '<div class="section-header">Sourcing Cost Drivers — Freight Burden by Category</div>',
        unsafe_allow_html=True,
    )

    cost_df = q("""
        SELECT product_category, total_items, avg_item_price, avg_freight_value,
               freight_pct_of_spend, freight_burden_tier
        FROM gold.gold_sourcing_cost_drivers
        ORDER BY freight_pct_of_spend DESC
        LIMIT 12
    """)

    if not cost_df.empty:
        colors = [
            "#ef4444" if t == "HIGH" else "#f59e0b" if t == "MEDIUM" else "#60a5fa"
            for t in cost_df["freight_burden_tier"]
        ]
        fig2 = go.Figure(go.Bar(
            x=cost_df["freight_pct_of_spend"],
            y=cost_df["product_category"],
            orientation="h",
            marker_color=colors,
            text=cost_df["freight_pct_of_spend"].apply(lambda v: f"{v:.1f}%"),
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Freight % of spend: %{x:.1f}%<extra></extra>",
        ))
        fig2.update_layout(
            xaxis_title="Freight as % of Category Spend",
            yaxis_title=None,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(t=20, b=40, l=10, r=20),
            height=340,
            font=dict(family="sans-serif", size=12),
            yaxis=dict(autorange="reversed", showgrid=False),
            xaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            showlegend=False,
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.caption(
            "🔴 HIGH (>25% of spend) · 🟠 MEDIUM (18.5–25%) · 🔵 LOW (≤18.5%) — "
            "tiers set from the dataset's own freight_pct_of_spend distribution, not fixed constants."
        )
    else:
        st.info("No sourcing cost driver data found.")

st.divider()

# ── 3c. Trade-Partner Concentration (UN Comtrade — live weekly pipeline) ───────
# Unlike every other section on this page (which reflects the last local
# `dbt build`), this section's underlying data is refreshed by
# dags/comtrade_weekly_dag.py on its own weekly schedule — re-running that pipeline
# (or `python -m ingestion.comtrade_pipeline`, manually) changes what renders here
# on the next page load, without touching this file. The "last refreshed" caption
# below exists specifically so that's visible, not just asserted.

st.markdown(
    '<div class="section-header">Trade-Partner Concentration — UN Comtrade (live)</div>',
    unsafe_allow_html=True,
)

COMMODITY_LABELS = {"85": "Electronics (HS 85)", "33": "Cosmetics (HS 33)", "94": "Furniture (HS 94)"}

commodities_df = q("""
    SELECT DISTINCT commodity_code FROM gold.gold_trade_concentration
    WHERE reporter_code = 76 ORDER BY 1
""")

if not commodities_df.empty:
    commodity_codes = commodities_df["commodity_code"].tolist()
    selected_commodity = st.selectbox(
        "Product (Brazil imports)",
        options=commodity_codes,
        format_func=lambda c: COMMODITY_LABELS.get(c, f"HS {c}"),
    )

    freshness = q(f"""
        SELECT max(_loaded_at) AS last_loaded, max(_run_id) AS last_run_id
        FROM silver.silver_comtrade_partner_flows
        WHERE reporter_code = 76 AND commodity_code = '{selected_commodity}'
    """)
    if not freshness.empty and freshness.iloc[0]["last_loaded"] is not None:
        st.caption(
            f"Last refreshed: {freshness.iloc[0]['last_loaded']} "
            f"(run `{freshness.iloc[0]['last_run_id']}`) — re-run "
            f"`python -m ingestion.comtrade_pipeline` or the weekly DAG to update this."
        )

    left3, right3 = st.columns([3, 2], gap="large")

    with left3:
        st.markdown("**Current concentration — latest period**")
        latest_period_df = q(f"""
            SELECT max(period) AS latest_period FROM gold.gold_trade_concentration
            WHERE reporter_code = 76 AND commodity_code = '{selected_commodity}'
        """)
        latest_period = latest_period_df.iloc[0]["latest_period"]

        top_partners = q(f"""
            SELECT partner_name, partner_share_pct, group_hhi, group_hhi_interpretation
            FROM gold.gold_trade_concentration
            WHERE reporter_code = 76 AND commodity_code = '{selected_commodity}' AND period = '{latest_period}'
            ORDER BY partner_rank LIMIT 10
        """)

        if not top_partners.empty:
            hhi = top_partners.iloc[0]["group_hhi"]
            interp = top_partners.iloc[0]["group_hhi_interpretation"]
            hhi_color = "danger" if interp == "HIGHLY_CONCENTRATED" else "warn" if interp == "MODERATELY_CONCENTRATED" else ""
            st.markdown(
                f'<div class="metric-card">'
                f'<div class="metric-label">HHI — {latest_period}</div>'
                f'<div class="metric-value {hhi_color}">{hhi:.0f}</div></div>',
                unsafe_allow_html=True,
            )
            st.caption(interp.replace("_", " ").title())

            fig3 = go.Figure(go.Bar(
                x=top_partners["partner_share_pct"],
                y=top_partners["partner_name"],
                orientation="h",
                marker_color="#60a5fa",
                text=top_partners["partner_share_pct"].apply(lambda v: f"{v:.1f}%"),
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>Share: %{x:.1f}%<extra></extra>",
            ))
            fig3.update_layout(
                xaxis_title="Share of import value (%)",
                yaxis_title=None,
                plot_bgcolor="white", paper_bgcolor="white",
                margin={"t": 10, "b": 40, "l": 10, "r": 20},
                height=320,
                font={"family": "sans-serif", "size": 12},
                yaxis={"autorange": "reversed", "showgrid": False},
                xaxis={"showgrid": True, "gridcolor": "#f1f5f9"},
                showlegend=False,
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No concentration data for this product yet.")

    with right3:
        st.markdown("**HHI trend across pulls**")
        trend = q(f"""
            SELECT DISTINCT period, group_hhi FROM gold.gold_trade_concentration
            WHERE reporter_code = 76 AND commodity_code = '{selected_commodity}'
            ORDER BY period
        """)
        if len(trend) >= 2:
            fig4 = go.Figure(go.Scatter(
                x=trend["period"], y=trend["group_hhi"],
                mode="lines+markers",
                line={"color": "#3b82f6", "width": 2},
                marker={"size": 8},
                hovertemplate="Period %{x}<br>HHI: %{y:.0f}<extra></extra>",
            ))
            fig4.add_hline(y=2500, line_dash="dot", line_color="#dc2626",
                            annotation_text="Highly concentrated (2500)", annotation_font_size=10)
            fig4.add_hline(y=1500, line_dash="dot", line_color="#d97706",
                            annotation_text="Moderately concentrated (1500)", annotation_font_size=10)
            fig4.update_layout(
                xaxis_title="Period", yaxis_title="HHI",
                plot_bgcolor="white", paper_bgcolor="white",
                margin={"t": 10, "b": 40, "l": 40, "r": 20},
                height=320,
                font={"family": "sans-serif", "size": 12},
                xaxis={"showgrid": False, "type": "category"},
                yaxis={"showgrid": True, "gridcolor": "#f1f5f9"},
                showlegend=False,
            )
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info(
                "Only one period pulled so far — a trend needs at least two. "
                "This is expected on a fresh pull, not missing data; run the "
                "pipeline again for a different period (or wait for next week's "
                "scheduled run) to see a trend line."
            )

    shift_df = q(f"""
        SELECT partner_name, prior_period, period, prior_partner_share_pct, partner_share_pct, partner_share_pct_change
        FROM gold.gold_trade_concentration_shift
        WHERE reporter_code = 76 AND commodity_code = '{selected_commodity}'
        ORDER BY abs(partner_share_pct_change) DESC LIMIT 8
    """)
    if not shift_df.empty:
        with st.expander("Biggest partner share shifts, period over period"):
            st.dataframe(
                shift_df.rename(columns={
                    "partner_name": "Partner", "prior_period": "From", "period": "To",
                    "prior_partner_share_pct": "Prior Share %", "partner_share_pct": "Current Share %",
                    "partner_share_pct_change": "Change (pp)",
                }),
                use_container_width=True, hide_index=True,
            )
else:
    st.info("No Comtrade trade-concentration data yet — run `python -m ingestion.comtrade_pipeline` and `dbt build`.")

st.divider()

# ── 4. AI Decision Assistant ───────────────────────────────────────────────────
# Calls the FastAPI /decisions/ask endpoint (agent/decision_agent.py — a direct
# Anthropic SDK tool-calling loop, not a placeholder). Requires the API to be
# running separately (see README) and ANTHROPIC_API_KEY set in its environment —
# this widget cannot import and call the agent in-process, since the dashboard and
# API are two separate deployable services (see docker-compose.yml / aws/ / gcp/).

st.markdown(
    '<div class="section-header">AI Decision Assistant</div>',
    unsafe_allow_html=True,
)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

question = st.text_input(
    label="Ask a supply chain question",
    placeholder="e.g. Which sellers are highest risk? Where is our concentration risk?",
)

if question:
    with st.spinner("Asking the decision agent..."):
        try:
            resp = requests.post(
                f"{API_BASE_URL}/decisions/ask",
                json={"question": question},
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()
        except requests.exceptions.RequestException as exc:
            result = None
            st.error(
                f"⚠️ Could not reach the decision agent at `{API_BASE_URL}` — "
                f"is the API running? ({exc})"
            )

    if result:
        st.markdown(f"**Answer:**\n\n{result.get('answer', '')}")

        action_items = result.get("action_items") or []
        if action_items:
            st.markdown("**Suggested actions:**")
            for item in action_items:
                st.markdown(f"- {item}")

        sql_used = result.get("sql_used") or []
        if sql_used:
            with st.expander(f"SQL the agent ran ({len(sql_used)} quer{'y' if len(sql_used) == 1 else 'ies'})"):
                for i, sql in enumerate(sql_used, 1):
                    st.code(sql.strip(), language="sql")
else:
    st.caption("Ask a question above to query the decision agent.")

# ── Footer ─────────────────────────────────────────────────────────────────────

st.markdown(
    "<br><center><small style='color:#94a3b8'>"
    "Supply Chain Decision Engine · DuckDB medallion lakehouse · gold layer"
    "</small></center>",
    unsafe_allow_html=True,
)
