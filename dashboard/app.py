"""Streamlit dashboard — Supply Chain Decision Engine."""

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")

st.set_page_config(
    page_title="Supply Chain Decision Engine",
    page_icon="🚚",
    layout="wide",
)

st.title("Supply Chain Decision Engine")


@st.cache_resource
def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


def query(sql: str) -> pd.DataFrame:
    try:
        return get_conn().execute(sql).fetchdf()
    except Exception as exc:
        st.error(f"Query failed: {exc}")
        return pd.DataFrame()


# --- Sidebar ---
st.sidebar.header("Filters")
state_filter = st.sidebar.text_input("Seller state (e.g. SP)", "")

# --- Supplier Performance ---
st.header("Supplier Performance")

supplier_sql = """
SELECT seller_id, seller_state, total_orders, avg_delivery_days,
       avg_review_score, total_revenue, freight_pct_of_revenue
FROM gold.gold_supplier_performance
"""
if state_filter:
    supplier_sql += f" WHERE seller_state = '{state_filter.upper()}'"
supplier_sql += " ORDER BY avg_review_score DESC NULLS LAST LIMIT 100"

df_suppliers = query(supplier_sql)
if not df_suppliers.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Suppliers", len(df_suppliers))
    col2.metric("Avg Review Score", round(df_suppliers["avg_review_score"].mean(), 2))
    col3.metric("Avg Delivery Days", round(df_suppliers["avg_delivery_days"].mean(), 1))

    fig = px.scatter(
        df_suppliers,
        x="avg_delivery_days",
        y="avg_review_score",
        size="total_orders",
        color="seller_state",
        hover_data=["seller_id", "total_revenue"],
        title="Delivery Speed vs Review Score",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df_suppliers, use_container_width=True)

# --- World Bank LPI ---
st.header("Logistics Performance Index (World Bank)")
df_lpi = query(
    """
    SELECT country_name, year, value AS lpi_overall
    FROM bronze.world_bank_lpi
    WHERE indicator_code = 'LP.LPI.OVRL.XQ'
      AND value IS NOT NULL
    ORDER BY year DESC, lpi_overall DESC
    LIMIT 100
    """
)
if not df_lpi.empty:
    fig2 = px.bar(
        df_lpi[df_lpi["year"] == df_lpi["year"].max()].head(20),
        x="country_name",
        y="lpi_overall",
        title=f"Top 20 Countries by LPI ({df_lpi['year'].max()})",
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- AI Assistant ---
st.header("AI Decision Assistant")
question = st.text_area("Ask a supply chain question:", height=80)
if st.button("Ask") and question:
    import httpx

    api_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    with st.spinner("Thinking..."):
        try:
            resp = httpx.post(f"{api_url}/decisions/ask", json={"question": question}, timeout=60)
            resp.raise_for_status()
            st.markdown(resp.json()["answer"])
        except Exception as exc:
            st.error(f"Agent error: {exc}")
