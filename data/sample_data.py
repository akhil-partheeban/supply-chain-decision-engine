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
            items.append({
                "order_id":           order_id,
                "order_item_id":      j,
                "product_id":         _uid(),
                "seller_id":          seller_id,
                "shipping_limit_date": carrier_pickup + timedelta(days=1),
                "price":              round(random.uniform(20, 500), 2),
                "freight_value":      round(random.uniform(5, 50), 2),
                "_source_file":       "sample_data",
                "_loaded_at":         datetime.utcnow(),
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

    # ── Silver: silver_sellers ──────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS silver.silver_sellers")
    conn.execute("""
        CREATE TABLE silver.silver_sellers AS
        WITH seller_orders AS (
            SELECT
                oi.seller_id,
                o.order_id,
                date_diff('day',
                    CAST(o.order_purchase_timestamp AS TIMESTAMP),
                    CAST(o.order_delivered_customer_date AS TIMESTAMP)
                ) AS delivery_days,
                CASE
                    WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
                    THEN 1 ELSE 0
                END AS is_late
            FROM bronze.order_items oi
            JOIN bronze.orders o ON oi.order_id = o.order_id
            WHERE oi.seller_id IS NOT NULL
        ),
        seller_reviews AS (
            SELECT
                oi.seller_id,
                AVG(CAST(r.review_score AS DOUBLE)) AS avg_review_score
            FROM bronze.order_items oi
            JOIN bronze.order_reviews r ON oi.order_id = r.order_id
            WHERE oi.seller_id IS NOT NULL
            GROUP BY oi.seller_id
        )
        SELECT
            s.seller_id,
            s.seller_city,
            s.seller_state,
            COUNT(DISTINCT so.order_id)                     AS total_orders,
            AVG(CAST(so.is_late AS DOUBLE))                 AS late_delivery_rate,
            AVG(so.delivery_days)                           AS avg_delivery_days,
            sr.avg_review_score
        FROM bronze.sellers s
        LEFT JOIN seller_orders so  ON s.seller_id = so.seller_id
        LEFT JOIN seller_reviews sr ON s.seller_id = sr.seller_id
        GROUP BY s.seller_id, s.seller_city, s.seller_state, sr.avg_review_score
    """)

    # ── Silver: silver_orders ───────────────────────────────────────────────────
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
            ) AS promised_delivery_days
        FROM bronze.orders
        WHERE order_id IS NOT NULL
    """)

    # ── Gold: supplier risk ─────────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS gold.gold_supplier_risk")
    conn.execute("""
        CREATE TABLE gold.gold_supplier_risk AS
        SELECT
            seller_id,
            seller_city,
            seller_state,
            total_orders,
            ROUND(late_delivery_rate, 4)  AS late_delivery_rate,
            ROUND(avg_review_score, 4)    AS avg_review_score,
            ROUND(avg_delivery_days, 2)   AS avg_delivery_days,
            CASE
                WHEN late_delivery_rate > 0.3  THEN 'HIGH'
                WHEN late_delivery_rate > 0.15 THEN 'MEDIUM'
                ELSE                                'LOW'
            END AS risk_tier
        FROM silver.silver_sellers
        WHERE total_orders > 0
    """)

    # ── Gold: concentration risk ────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS gold.gold_concentration_risk")
    conn.execute("""
        CREATE TABLE gold.gold_concentration_risk AS
        WITH state_totals AS (
            SELECT
                seller_state,
                COUNT(DISTINCT seller_id) AS total_sellers,
                SUM(total_orders)         AS total_orders
            FROM silver.silver_sellers
            WHERE total_orders > 0
            GROUP BY seller_state
        ),
        grand_total AS (
            SELECT SUM(total_orders) AS grand_total_orders
            FROM silver.silver_sellers
            WHERE total_orders > 0
        )
        SELECT
            st.seller_state,
            st.total_sellers,
            st.total_orders,
            ROUND(st.total_orders / gt.grand_total_orders, 4) AS pct_of_total_orders,
            CASE
                WHEN st.total_orders / gt.grand_total_orders > 0.2 THEN 'HIGH'
                ELSE 'NORMAL'
            END AS concentration_flag
        FROM state_totals st
        CROSS JOIN grand_total gt
        ORDER BY st.total_orders DESC
    """)

    # ── Gold: executive summary ─────────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS gold.gold_executive_summary")
    conn.execute("""
        CREATE TABLE gold.gold_executive_summary AS
        SELECT
            SUM(total_orders)                                                       AS total_orders,
            COUNT(DISTINCT seller_id)                                               AS total_sellers,
            ROUND(SUM(total_orders * late_delivery_rate)
                  / NULLIF(SUM(total_orders), 0), 4)                               AS overall_late_rate,
            ROUND(
                COUNT(DISTINCT CASE WHEN late_delivery_rate > 0.3 THEN seller_id END)
                / NULLIF(COUNT(DISTINCT seller_id), 0)::DOUBLE, 4
            )                                                                       AS pct_high_risk_sellers,
            ROUND(AVG(avg_review_score), 4)                                         AS avg_review_score
        FROM silver.silver_sellers
        WHERE total_orders > 0
    """)

    conn.close()

    print(f"  gold.gold_supplier_risk, gold.gold_concentration_risk, gold.gold_executive_summary: built")
    print("Sample database ready.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample supply chain DuckDB")
    parser.add_argument("--db", default=None, help="Path to DuckDB file")
    args = parser.parse_args()
    build_sample_db(args.db)
