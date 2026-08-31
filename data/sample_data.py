"""
Generate synthetic but realistic Olist-like data and load it into DuckDB.

Produces 500 orders, 50 sellers across 10 Brazilian states, then computes
the silver and gold layers directly in SQL so the dashboard works without
the real Kaggle CSVs.

Usage:
    python -m data.sample_data                     # writes to default DB path
    python -m data.sample_data --db /tmp/test.duckdb
"""

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Constants ──────────────────────────────────────────────────────────────────

STATES = ["SP", "MG", "RJ", "PR", "SC", "RS", "BA", "GO", "DF", "CE"]
CITIES = {
    "SP": "sao paulo",     "MG": "belo horizonte", "RJ": "rio de janeiro",
    "PR": "curitiba",      "SC": "florianopolis",  "RS": "porto alegre",
    "BA": "salvador",      "GO": "goiania",         "DF": "brasilia",
    "CE": "fortaleza",
}
# Weight toward SP to mirror real Olist distribution
STATE_WEIGHTS = [0.40, 0.10, 0.08, 0.08, 0.07, 0.06, 0.05, 0.05, 0.06, 0.05]

# category -> (min_g, max_g); weight range drives the synthetic freight-per-kg spread
# that gold_sourcing_cost_drivers reports on.
CATEGORIES = {
    "electronics":        (200, 3000),
    "furniture":           (5000, 40000),
    "health_beauty":      (100, 1500),
    "sports_leisure":     (300, 8000),
    "housewares":         (500, 12000),
    "toys":               (200, 4000),
    "fashion":            (100, 1200),
}

N_SELLERS = 50
N_ORDERS  = 500

random.seed(42)


# ── Data generators ────────────────────────────────────────────────────────────

def _uid() -> str:
    return str(uuid.uuid4()).replace("-", "")


def _make_sellers() -> pd.DataFrame:
    states = random.choices(STATES, weights=STATE_WEIGHTS, k=N_SELLERS)
    return pd.DataFrame({
        "seller_id":             [_uid() for _ in range(N_SELLERS)],
        "seller_zip_code_prefix": [f"{random.randint(10000, 99999)}" for _ in range(N_SELLERS)],
        "seller_city":           [CITIES[s] for s in states],
        "seller_state":          states,
        "_source_file":          "sample_data",
        "_loaded_at":            datetime.utcnow(),
    })


def _make_orders(seller_ids: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    orders, items, reviews = [], [], []

    for i in range(N_ORDERS):
        order_id   = _uid()
        customer_id = _uid()

        # Purchase in 2017-2018
        purchase = datetime(2017, 1, 1) + timedelta(
            days=random.randint(0, 730),
            hours=random.randint(8, 22),
            minutes=random.randint(0, 59),
        )
        approved       = purchase + timedelta(hours=random.randint(1, 8))
        carrier_pickup = approved + timedelta(days=random.randint(1, 3))
        estimated      = purchase + timedelta(days=random.randint(10, 25))

        # ~12 % late deliveries
        if random.random() < 0.12:
            delivered = estimated + timedelta(days=random.randint(1, 10))
        else:
            delivered = purchase + timedelta(days=random.randint(5, int((estimated - purchase).days)))

        orders.append({
            "order_id":                    order_id,
            "customer_id":                 customer_id,
            "order_status":                "delivered",
            "order_purchase_timestamp":    purchase,
            "order_approved_at":           approved,
            "order_delivered_carrier_date": carrier_pickup,
            "order_delivered_customer_date": delivered,
            "order_estimated_delivery_date": estimated,
            "_source_file":                "sample_data",
            "_loaded_at":                  datetime.utcnow(),
        })

        # 1-2 items per order
        n_items = random.choices([1, 2], weights=[0.75, 0.25])[0]
        used_sellers = random.sample(seller_ids, min(n_items, len(seller_ids)))
        for j, seller_id in enumerate(used_sellers, 1):
            category = random.choice(list(CATEGORIES))
            weight_g = random.randint(*CATEGORIES[category])
            price = round(random.uniform(20, 500), 2)
            # Freight scales with weight (heavier => costlier to ship) plus noise, so
            # gold_sourcing_cost_drivers produces a real spread of freight_pct_of_spend
            # across categories rather than uniform noise.
            freight_value = round(max(5.0, weight_g / 1000 * random.uniform(3, 7)), 2)
            items.append({
                "order_id":            order_id,
                "order_item_id":       j,
                "product_id":          _uid(),
                "seller_id":           seller_id,
                "shipping_limit_date": carrier_pickup + timedelta(days=1),
                "price":               price,
                "freight_value":       freight_value,
                "product_category":    category,
                "product_weight_g":    weight_g,
                "_source_file":        "sample_data",
                "_loaded_at":          datetime.utcnow(),
            })

        # Review score: weighted toward 4-5
        score = random.choices([1, 2, 3, 4, 5], weights=[0.03, 0.05, 0.12, 0.30, 0.50])[0]
        reviews.append({
            "review_id":              _uid(),
            "order_id":               order_id,
            "review_score":           score,
            "review_comment_title":   "",
            "review_comment_message": "",
            "review_creation_date":   delivered + timedelta(days=random.randint(1, 5)),
            "review_answer_timestamp": delivered + timedelta(days=random.randint(2, 7)),
            "_source_file":           "sample_data",
            "_loaded_at":             datetime.utcnow(),
        })

    return (
        pd.DataFrame(orders),
        pd.DataFrame(items),
        pd.DataFrame(reviews),
    )


# ── DB builder ─────────────────────────────────────────────────────────────────

def build_sample_db(db_path: str | None = None) -> None:
    if db_path is None:
        db_path = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    print(f"Building sample database → {db_path}")

    sellers_df = _make_sellers()
    orders_df, items_df, reviews_df = _make_orders(sellers_df["seller_id"].tolist())

    conn = duckdb.connect(db_path)

    # ── Bronze ──────────────────────────────────────────────────────────────────
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

    for tbl, df in [
        ("sellers",      sellers_df),
        ("orders",       orders_df),
        ("order_items",  items_df),
        ("order_reviews", reviews_df),
    ]:
        conn.execute(f"DROP TABLE IF EXISTS bronze.{tbl}")
        conn.execute(f"CREATE TABLE bronze.{tbl} AS SELECT * FROM df")
        n = conn.execute(f"SELECT COUNT(*) FROM bronze.{tbl}").fetchone()[0]
        print(f"  bronze.{tbl}: {n:,} rows")

    # NOTE: the real pipeline (see CLAUDE.md) builds silver/gold with dbt models under
    # dbt/models/. Streamlit Community Cloud only runs streamlit_app.py — it can't shell
    # out to `dbt run` — so this function re-implements the same silver/gold SQL directly
    # against the synthetic bronze tables above. The formulas below are kept in lockstep
    # with dbt/models/{silver,gold}/*.sql by hand; see DECISIONS.md for why this
    # duplication exists and what would break the two copies apart if left unmaintained.

    # ── Silver: silver_orders (mirrors dbt/models/silver/silver_orders.sql) ───────
    conn.execute("DROP TABLE IF EXISTS silver.silver_orders")
    conn.execute("""
        CREATE TABLE silver.silver_orders AS
        SELECT
            order_id,
            customer_id,
            order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP)      AS purchased_at,
            CAST(order_approved_at AS TIMESTAMP)             AS approved_at,
            CAST(order_delivered_carrier_date AS TIMESTAMP)  AS carrier_pickup_at,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_at,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at,
            date_diff('day',
                CAST(order_purchase_timestamp AS TIMESTAMP),
                CAST(order_delivered_customer_date AS TIMESTAMP)
            ) AS actual_delivery_days,
            date_diff('day',
                CAST(order_purchase_timestamp AS TIMESTAMP),
                CAST(order_estimated_delivery_date AS TIMESTAMP)
            ) AS promised_delivery_days,
            CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END AS is_delivered,
            CASE
                WHEN order_delivered_customer_date IS NOT NULL
                 AND order_delivered_customer_date > order_estimated_delivery_date
                THEN 1 ELSE 0
            END AS is_late
        FROM bronze.orders
        WHERE order_id IS NOT NULL
    """)

    # ── Silver: silver_sellers (dimension only — mirrors silver_sellers.sql) ──────
    conn.execute("DROP TABLE IF EXISTS silver.silver_sellers")
    conn.execute("""
        CREATE TABLE silver.silver_sellers AS
        SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
        FROM bronze.sellers
        WHERE seller_id IS NOT NULL
    """)

    # ── Gold: supplier scorecard (mirrors gold_supplier_scorecard.sql) ────────────
    conn.execute("DROP TABLE IF EXISTS gold.gold_supplier_scorecard")
    conn.execute("""
        CREATE TABLE gold.gold_supplier_scorecard AS
        WITH seller_orders AS (
            SELECT DISTINCT
                oi.seller_id, o.order_id, o.is_delivered, o.is_late, o.actual_delivery_days
            FROM bronze.order_items oi
            JOIN silver.silver_orders o USING (order_id)
        ),
        seller_reviews AS (
            SELECT DISTINCT oi.seller_id, r.order_id, r.review_score
            FROM bronze.order_items oi
            JOIN bronze.order_reviews r USING (order_id)
        ),
        item_agg AS (
            SELECT
                seller_id,
                count(DISTINCT order_id)   AS total_orders,
                count(DISTINCT product_id) AS unique_products,
                count(DISTINCT product_category) AS unique_categories,
                round(sum(price), 2)       AS total_revenue,
                round(sum(freight_value), 2) AS total_freight_cost
            FROM bronze.order_items GROUP BY seller_id
        ),
        delivery_agg AS (
            SELECT
                seller_id,
                round(avg(actual_delivery_days) FILTER (WHERE is_delivered = 1), 2) AS avg_delivery_days,
                round(stddev_samp(actual_delivery_days) FILTER (WHERE is_delivered = 1), 2) AS delivery_days_stddev,
                round(avg(is_late::DOUBLE), 4) AS late_delivery_rate
            FROM seller_orders GROUP BY seller_id
        ),
        review_agg AS (
            SELECT
                seller_id,
                count(*) AS review_count,
                round(avg(review_score), 2) AS avg_review_score,
                round(sum(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) / nullif(count(*), 0)::DOUBLE, 4) AS pct_negative_reviews
            FROM seller_reviews GROUP BY seller_id
        ),
        scored AS (
            SELECT
                s.seller_id, s.seller_city, s.seller_state,
                ia.total_orders, ia.unique_products, ia.unique_categories,
                ia.total_revenue, ia.total_freight_cost,
                round(ia.total_freight_cost / nullif(ia.total_revenue, 0) * 100, 2) AS freight_pct_of_revenue,
                da.avg_delivery_days,
                coalesce(da.delivery_days_stddev, 0) AS delivery_days_stddev,
                coalesce(da.late_delivery_rate, 0)   AS late_delivery_rate,
                round(1 - coalesce(da.late_delivery_rate, 0), 4) AS on_time_rate,
                ra.review_count,
                coalesce(ra.avg_review_score, 0)     AS avg_review_score,
                coalesce(ra.pct_negative_reviews, 0) AS pct_negative_reviews,
                round(
                    50 * (1 - coalesce(da.late_delivery_rate, 0))
                  + 30 * (coalesce(ra.avg_review_score, 0) / 5.0)
                  + 20 * greatest(0, 1 - coalesce(da.delivery_days_stddev, 0) / 20.0),
                1) AS reliability_score
            FROM silver.silver_sellers s
            JOIN item_agg ia USING (seller_id)
            LEFT JOIN delivery_agg da USING (seller_id)
            LEFT JOIN review_agg ra USING (seller_id)
        )
        SELECT *,
            CASE
                WHEN reliability_score >= 85 THEN 'LOW'
                WHEN reliability_score >= 70 THEN 'MEDIUM'
                ELSE 'HIGH'
            END AS risk_tier
        FROM scored
    """)

    # ── Gold: supplier concentration / HHI (mirrors gold_concentration_risk.sql) ──
    conn.execute("DROP TABLE IF EXISTS gold.gold_concentration_risk")
    conn.execute("""
        CREATE TABLE gold.gold_concentration_risk AS
        WITH seller_revenue AS (
            SELECT seller_id, round(sum(price), 2) AS total_revenue
            FROM bronze.order_items GROUP BY seller_id
        ),
        totals AS (SELECT sum(total_revenue) AS grand_total_revenue FROM seller_revenue),
        shared AS (
            SELECT sr.seller_id, s.seller_state, sr.total_revenue,
                round(sr.total_revenue / t.grand_total_revenue * 100, 4) AS revenue_share_pct
            FROM seller_revenue sr
            CROSS JOIN totals t
            LEFT JOIN silver.silver_sellers s USING (seller_id)
        )
        SELECT
            seller_id, seller_state, total_revenue, revenue_share_pct,
            round(power(revenue_share_pct, 2), 4) AS hhi_contribution,
            round(sum(revenue_share_pct) OVER (ORDER BY revenue_share_pct DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_share_pct,
            row_number() OVER (ORDER BY revenue_share_pct DESC) AS revenue_rank,
            CASE WHEN revenue_share_pct > 10 THEN 'HIGH' ELSE 'NORMAL' END AS concentration_flag
        FROM shared
        ORDER BY revenue_share_pct DESC
    """)

    # ── Gold: geographic concentration (mirrors gold_geo_concentration.sql) ───────
    conn.execute("DROP TABLE IF EXISTS gold.gold_geo_concentration")
    conn.execute("""
        CREATE TABLE gold.gold_geo_concentration AS
        WITH state_totals AS (
            SELECT s.seller_state, count(DISTINCT s.seller_id) AS total_sellers,
                round(sum(oi.price), 2) AS total_revenue
            FROM silver.silver_sellers s
            JOIN bronze.order_items oi USING (seller_id)
            GROUP BY s.seller_state
        ),
        grand_total AS (SELECT sum(total_revenue) AS grand_total_revenue FROM state_totals)
        SELECT
            st.seller_state, st.total_sellers, st.total_revenue,
            round(st.total_revenue / gt.grand_total_revenue * 100, 2) AS pct_of_total_revenue,
            CASE WHEN st.total_revenue / gt.grand_total_revenue > 0.2 THEN 'HIGH' ELSE 'NORMAL' END AS concentration_flag
        FROM state_totals st CROSS JOIN grand_total gt
        ORDER BY st.total_revenue DESC
    """)

    # ── Gold: sourcing cost drivers (mirrors gold_sourcing_cost_drivers.sql) ──────
    conn.execute("DROP TABLE IF EXISTS gold.gold_sourcing_cost_drivers")
    conn.execute("""
        CREATE TABLE gold.gold_sourcing_cost_drivers AS
        WITH category_agg AS (
            SELECT
                product_category,
                count(*) AS total_items,
                count(DISTINCT seller_id) AS seller_count,
                round(sum(price), 2) AS total_spend,
                round(sum(freight_value), 2) AS total_freight_cost,
                round(avg(price), 2) AS avg_item_price,
                round(avg(freight_value), 2) AS avg_freight_value,
                round(avg(freight_value / nullif(product_weight_g / 1000.0, 0)), 4) AS avg_freight_cost_per_kg
            FROM bronze.order_items
            WHERE product_category IS NOT NULL
            GROUP BY product_category
        )
        SELECT *,
            round(total_freight_cost / nullif(total_spend, 0) * 100, 2) AS freight_pct_of_spend,
            CASE
                WHEN total_freight_cost / nullif(total_spend, 0) * 100 > 25   THEN 'HIGH'
                WHEN total_freight_cost / nullif(total_spend, 0) * 100 > 18.5 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS freight_burden_tier
        FROM category_agg
        ORDER BY freight_pct_of_spend DESC
    """)

    # ── Gold: executive summary (mirrors gold_executive_summary.sql) ──────────────
    conn.execute("DROP TABLE IF EXISTS gold.gold_executive_summary")
    conn.execute("""
        CREATE TABLE gold.gold_executive_summary AS
        WITH scorecard_agg AS (
            SELECT
                sum(total_orders) AS total_orders,
                count(DISTINCT seller_id) AS total_suppliers,
                sum(unique_products) AS total_product_listings,
                round(sum(total_orders * late_delivery_rate) / nullif(sum(total_orders), 0), 4) AS overall_late_rate,
                round(avg(reliability_score), 1) AS avg_reliability_score,
                round(count(*) FILTER (WHERE risk_tier = 'HIGH') / nullif(count(*), 0)::DOUBLE, 4) AS pct_high_risk_suppliers,
                round(sum(review_count * avg_review_score) / nullif(sum(review_count), 0), 4) AS avg_review_score,
                round(sum(total_revenue), 2) AS total_revenue
            FROM gold.gold_supplier_scorecard
        ),
        concentration_agg AS (
            SELECT
                round(sum(hhi_contribution), 2) AS hhi_index,
                round(sum(revenue_share_pct) FILTER (WHERE revenue_rank <= 5), 2) AS top5_supplier_revenue_share_pct
            FROM gold.gold_concentration_risk
        ),
        cost_agg AS (SELECT count(*) AS categories_scored FROM gold.gold_sourcing_cost_drivers)
        SELECT
            sa.total_orders, sa.total_suppliers, sa.total_product_listings, sa.total_revenue,
            sa.overall_late_rate, sa.avg_reliability_score, sa.pct_high_risk_suppliers, sa.avg_review_score,
            ca.hhi_index,
            CASE
                WHEN ca.hhi_index >= 2500 THEN 'HIGHLY_CONCENTRATED'
                WHEN ca.hhi_index >= 1500 THEN 'MODERATELY_CONCENTRATED'
                ELSE 'UNCONCENTRATED'
            END AS hhi_interpretation,
            ca.top5_supplier_revenue_share_pct, co.categories_scored
        FROM scorecard_agg sa CROSS JOIN concentration_agg ca CROSS JOIN cost_agg co
    """)

    # ── Gold: trade concentration (mirrors gold_trade_concentration.sql / _shift.sql) ─
    # Phase 6's real pipeline (ingestion/comtrade_pipeline.py + dags/comtrade_weekly_dag.py)
    # pulls live UN Comtrade data — not reproducible here without network access
    # during a Cloud cold start. This synthetic block exists so the Cloud dashboard's
    # "Trade-Partner Concentration" section has something to render rather than
    # falling into its "no Comtrade data yet" empty state — the two example products
    # below are hand-picked to show one HIGHLY_CONCENTRATED and one UNCONCENTRATED
    # case, matching the real pipeline's own actual finding (Brazil's electronics
    # imports are dominated by a single partner; cosmetics are not) rather than an
    # arbitrary distribution — see DECISIONS.md, Phase 6, for the real numbers this
    # is modeled on.
    trade_partners = {
        "85": [("China", 52.0), ("USA", 7.0), ("Germany", 4.0), ("Japan", 3.0), ("Other", 34.0)],
        "33": [("USA", 12.0), ("France", 11.0), ("China", 10.0), ("Germany", 9.0), ("Other", 58.0)],
    }
    concentration_rows = []
    for period in ("2024", "2025"):
        for commodity, partners in trade_partners.items():
            hhi = round(sum(share ** 2 for _, share in partners), 2)
            interp = "HIGHLY_CONCENTRATED" if hhi >= 2500 else "MODERATELY_CONCENTRATED" if hhi >= 1500 else "UNCONCENTRATED"
            cumulative = 0.0
            for rank, (partner, share) in enumerate(sorted(partners, key=lambda p: -p[1]), start=1):
                cumulative += share
                concentration_rows.append({
                    "reporter_code": 76, "period": period, "flow_type": "import",
                    "commodity_code": commodity, "partner_code": rank, "partner_name": partner,
                    "fob_value_usd": share * 1_000_000, "partner_share_pct": share,
                    "hhi_contribution": round(share ** 2, 2), "partner_rank": rank,
                    "cumulative_share_pct": round(cumulative, 2), "group_hhi": hhi,
                    "group_hhi_interpretation": interp,
                })
    concentration_df = pd.DataFrame(concentration_rows)
    conn.execute("DROP TABLE IF EXISTS gold.gold_trade_concentration")
    conn.execute("CREATE TABLE gold.gold_trade_concentration AS SELECT * FROM concentration_df")

    # Minimal synthetic silver.silver_comtrade_partner_flows — the dashboard's
    # "last refreshed" caption for this section reads _loaded_at/_run_id from here,
    # not from the gold table. Not a full replica of the real model's columns,
    # just enough for that one query to resolve instead of erroring on Cloud.
    freshness_df = concentration_df[["reporter_code", "commodity_code"]].drop_duplicates().copy()
    freshness_df["_loaded_at"] = datetime.utcnow()
    freshness_df["_run_id"] = "sample-data-bootstrap"
    conn.execute("DROP TABLE IF EXISTS silver.silver_comtrade_partner_flows")
    conn.execute("CREATE TABLE silver.silver_comtrade_partner_flows AS SELECT * FROM freshness_df")

    conn.execute("DROP TABLE IF EXISTS gold.gold_trade_concentration_shift")
    conn.execute("""
        CREATE TABLE gold.gold_trade_concentration_shift AS
        WITH with_prior AS (
            SELECT *,
                lag(period) OVER (PARTITION BY reporter_code, commodity_code, flow_type, partner_code ORDER BY period) AS prior_period,
                lag(partner_share_pct) OVER (PARTITION BY reporter_code, commodity_code, flow_type, partner_code ORDER BY period) AS prior_partner_share_pct,
                lag(group_hhi) OVER (PARTITION BY reporter_code, commodity_code, flow_type, partner_code ORDER BY period) AS prior_group_hhi
            FROM gold.gold_trade_concentration
        )
        SELECT
            reporter_code, commodity_code, flow_type, period, prior_period, partner_code, partner_name,
            partner_share_pct, prior_partner_share_pct,
            round(partner_share_pct - prior_partner_share_pct, 2) AS partner_share_pct_change,
            group_hhi, prior_group_hhi,
            round(group_hhi - prior_group_hhi, 2) AS group_hhi_change,
            group_hhi_interpretation
        FROM with_prior
        WHERE prior_period IS NOT NULL
    """)

    conn.close()

    print(
        "  gold.gold_supplier_scorecard, gold.gold_concentration_risk, "
        "gold.gold_geo_concentration, gold.gold_sourcing_cost_drivers, "
        "gold.gold_trade_concentration, gold.gold_trade_concentration_shift, "
        "gold.gold_executive_summary: built"
    )
    print("Sample database ready.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample supply chain DuckDB")
    parser.add_argument("--db", default=None, help="Path to DuckDB file")
    args = parser.parse_args()
    build_sample_db(args.db)
