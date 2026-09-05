"""
Build a small, fully synthetic DuckDB bronze layer so `dbt build` (models + tests)
can run deterministically in CI — no Kaggle download, no live Comtrade/World Bank
API calls, no network dependency of any kind.

Why this exists, and why it's a different thing from data/sample_data.py:
data/sample_data.py's job is producing a demo-quality database for the Streamlit
Community Cloud deployment — it doesn't build every bronze source dbt's models
reference (no products/product_category_name_translation/order_reviews/comtrade
partner-level/world_bank_lpi tables), and its own gold tables are hand-computed in
raw SQL rather than run through the actual dbt models. Running `dbt build` against
it would fail to compile: several models select from bronze sources that dataset
never creates.

This script instead creates every bronze table referenced anywhere in
dbt/models/**/*.sql (checked directly via `grep -rho "source('bronze', '...')"`
across the project — 8 tables are actually used, out of 11 declared in
sources.yml), with the exact columns each model selects, in small enough volume to
be obviously synthetic but large enough to exercise real branches in the SQL:
multiple risk tiers, a category that clears gold_sourcing_cost_drivers' >=30-item
threshold and one that doesn't, two Comtrade periods so the shift/LAG logic in
gold_trade_concentration_shift produces real (non-empty) rows, and a seller cohort
that straddles the 2018-01-01 backtest split so gold_risk_score_validation has
something to validate.

See DECISIONS.md, "CI/CD," for why a synthetic fixture is the correct call here
rather than pulling real data in CI (Olist CSVs are gitignored/licensed, and the
live Comtrade/World Bank APIs are rate-limited and non-deterministic — neither is
appropriate for a required, blocking CI gate).

Usage:
    python -m scripts.build_ci_fixture_db --db path/to/ci_fixture.duckdb
"""

import argparse
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd

FIXED_LOADED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _dt(days_from_purchase: int, base: datetime) -> datetime:
    return base + timedelta(days=days_from_purchase)


def build_orders_and_items() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Six sellers, deliberately varied so downstream gold models see real
    diversity rather than one degenerate case:
      S1 reliable, straddles the 2018-01-01 backtest split (4 pre + 2 post orders)
      S2 unreliable, also straddles the split (4 pre + 2 post, mostly late)
      S3 pre-cutoff only (3 orders) -> excluded from the backtest (no holdout orders)
      S4 too few orders either side (1 + 1) -> excluded from the backtest (training < 3)
      S5, S6 post-cutoff only, higher volume -> pushes the 'electronics_fixture'
             category over gold_sourcing_cost_drivers' >=30-item bar while
             'furniture_fixture' (S4 + S6) deliberately stays under it, so both
             branches of that model's HAVING filter are genuinely exercised.
    """
    sellers = pd.DataFrame([
        {"seller_id": "S1", "seller_zip_code_prefix": "10000", "seller_city": "sao paulo", "seller_state": "SP",
         "_source_file": "ci_fixture.csv", "_loaded_at": FIXED_LOADED_AT},
        {"seller_id": "S2", "seller_zip_code_prefix": "20000", "seller_city": "rio de janeiro", "seller_state": "RJ",
         "_source_file": "ci_fixture.csv", "_loaded_at": FIXED_LOADED_AT},
        {"seller_id": "S3", "seller_zip_code_prefix": "30000", "seller_city": "belo horizonte", "seller_state": "MG",
         "_source_file": "ci_fixture.csv", "_loaded_at": FIXED_LOADED_AT},
        {"seller_id": "S4", "seller_zip_code_prefix": "10001", "seller_city": "sao paulo", "seller_state": "SP",
         "_source_file": "ci_fixture.csv", "_loaded_at": FIXED_LOADED_AT},
        {"seller_id": "S5", "seller_zip_code_prefix": "20001", "seller_city": "rio de janeiro", "seller_state": "RJ",
         "_source_file": "ci_fixture.csv", "_loaded_at": FIXED_LOADED_AT},
        {"seller_id": "S6", "seller_zip_code_prefix": "30001", "seller_city": "belo horizonte", "seller_state": "MG",
         "_source_file": "ci_fixture.csv", "_loaded_at": FIXED_LOADED_AT},
    ])

    orders: list[dict] = []
    items: list[dict] = []
    reviews: list[dict] = []
    order_seq = 0

    def add_order(seller_id: str, purchase: datetime, late: bool, category: str, review_score: int) -> None:
        nonlocal order_seq
        order_seq += 1
        order_id = f"O{order_seq:04d}"
        estimated = purchase + timedelta(days=10)
        delivered = estimated + timedelta(days=3) if late else purchase + timedelta(days=5)
        orders.append({
            "order_id": order_id,
            "customer_id": f"C{order_seq:04d}",
            "order_status": "delivered",
            "order_purchase_timestamp": purchase,
            "order_approved_at": purchase + timedelta(hours=2),
            "order_delivered_carrier_date": purchase + timedelta(days=1),
            "order_delivered_customer_date": delivered,
            "order_estimated_delivery_date": estimated,
            "_source_file": "ci_fixture.csv",
            "_loaded_at": FIXED_LOADED_AT,
        })
        product_id = "P_ELEC" if category == "electronics_fixture" else "P_FURN"
        items.append({
            "order_id": order_id,
            "order_item_id": 1,
            "product_id": product_id,
            "seller_id": seller_id,
            "price": 100.0,
            "freight_value": 15.0,
        })
        reviews.append({
            "review_id": f"R{order_seq:04d}",
            "order_id": order_id,
            "review_score": review_score,
        })

    pre_cutoff = datetime(2017, 6, 1)
    post_cutoff = datetime(2018, 3, 1)

    # S1 — reliable, straddles the backtest split: on-time, good reviews both sides.
    for i in range(4):
        add_order("S1", pre_cutoff + timedelta(days=i * 20), late=False, category="electronics_fixture", review_score=5)
    for i in range(2):
        add_order("S1", post_cutoff + timedelta(days=i * 20), late=False, category="electronics_fixture", review_score=5)

    # S2 — unreliable, straddles the split: mostly late, poor reviews both sides.
    for i in range(4):
        add_order("S2", pre_cutoff + timedelta(days=i * 20), late=True, category="electronics_fixture", review_score=2)
    for i in range(2):
        add_order("S2", post_cutoff + timedelta(days=i * 20), late=True, category="electronics_fixture", review_score=2)

    # S3 — pre-cutoff only: 3 training orders, 0 holdout -> excluded from the backtest.
    for i in range(3):
        add_order("S3", pre_cutoff + timedelta(days=i * 15), late=False, category="electronics_fixture", review_score=4)

    # S4 — too few orders on either side of the split (1 + 1): training < 3, excluded.
    add_order("S4", pre_cutoff, late=False, category="furniture_fixture", review_score=4)
    add_order("S4", post_cutoff, late=False, category="furniture_fixture", review_score=4)

    # S5 — post-cutoff only, high volume: pushes 'electronics_fixture' past the
    # gold_sourcing_cost_drivers >=30-item bar (31 electronics_fixture items total
    # across S1/S2/S3/S5) and adds a MEDIUM-ish risk tier for variety.
    for i in range(16):
        add_order("S5", post_cutoff + timedelta(days=i * 5), late=(i % 3 == 0), category="electronics_fixture", review_score=4 if i % 3 else 3)

    # S6 — post-cutoff only: keeps 'furniture_fixture' deliberately under the
    # 30-item bar (10 total with S4), so that branch of the HAVING filter is
    # exercised too (a category that correctly does NOT appear in the gold model).
    for i in range(8):
        add_order("S6", post_cutoff + timedelta(days=i * 5), late=(i % 4 == 0), category="furniture_fixture", review_score=5 if i % 4 else 2)

    return sellers, pd.DataFrame(orders), pd.DataFrame(items), pd.DataFrame(reviews)


def build_products() -> tuple[pd.DataFrame, pd.DataFrame]:
    products = pd.DataFrame([
        {"product_id": "P_ELEC", "product_category_name": "eletronicos_fixture", "product_weight_g": 500},
        {"product_id": "P_FURN", "product_category_name": "moveis_fixture", "product_weight_g": 8000},
    ])
    translation = pd.DataFrame([
        {"product_category_name": "eletronicos_fixture", "product_category_name_english": "electronics_fixture"},
        {"product_category_name": "moveis_fixture", "product_category_name_english": "furniture_fixture"},
    ])
    return products, translation


def build_comtrade() -> pd.DataFrame:
    """World-aggregate rows (partnerCode=0, cmdCode='TOTAL') feed gold_trade_balance
    / gold_macro_context; partner-level rows (real country codes from the actual
    comtrade_partner_areas seed, so the join in silver_comtrade_partner_flows
    resolves to a real name) across two periods feed gold_trade_concentration and
    give gold_trade_concentration_shift's LAG() something to compare against.
    """
    rows: list[dict] = []

    def add(reporter, period, flow, cmd, partner, mot, fob, cif):
        rows.append({
            "reporterCode": reporter, "period": period, "flowCode": flow, "cmdCode": cmd,
            "partnerCode": partner, "motCode": mot, "fobvalue": fob, "cifvalue": cif,
            "_loaded_at": FIXED_LOADED_AT, "_run_id": "ci-fixture",
        })

    for period, mult in (("2024", 1.0), ("2025", 1.1)):
        add(76, period, "M", "TOTAL", 0, 0, 1_000_000 * mult, 1_050_000 * mult)
        add(76, period, "X", "TOTAL", 0, 0, 1_200_000 * mult, 1_260_000 * mult)
        # Electronics (85): China (156) dominant, mirroring the real project's own
        # finding, so this fixture's HHI is genuinely concentrated, not degenerate.
        add(76, period, "M", "85", 156, 0, 520_000 * mult, 540_000 * mult)
        add(76, period, "M", "85", 842, 0, 90_000 * mult, 94_000 * mult)
        add(76, period, "M", "85", 276, 0, 60_000 * mult, 63_000 * mult)
        # Cosmetics (33): spread evenly -> genuinely unconcentrated, the other real branch.
        add(76, period, "M", "33", 156, 0, 40_000 * mult, 42_000 * mult)
        add(76, period, "M", "33", 842, 0, 38_000 * mult, 40_000 * mult)
        add(76, period, "M", "33", 276, 0, 36_000 * mult, 38_000 * mult)

    return pd.DataFrame(rows)


def build_world_bank_lpi() -> pd.DataFrame:
    return pd.DataFrame([
        {"country_code": "BRA", "country_name": "Brazil", "indicator_code": "LP.LPI.OVRL.XQ",
         "indicator_name": "Logistics performance index: Overall", "year": 2022, "value": 3.2,
         "_loaded_at": FIXED_LOADED_AT},
        {"country_code": "ARG", "country_name": "Argentina", "indicator_code": "LP.LPI.OVRL.XQ",
         "indicator_name": "Logistics performance index: Overall", "year": 2022, "value": 2.8,
         "_loaded_at": FIXED_LOADED_AT},
    ])


def build_fixture_db(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    conn.execute("DROP SCHEMA IF EXISTS bronze CASCADE")
    conn.execute("CREATE SCHEMA bronze")

    sellers, orders, items, reviews = build_orders_and_items()
    products, translation = build_products()
    comtrade = build_comtrade()
    lpi = build_world_bank_lpi()

    tables = {
        "sellers": sellers,
        "orders": orders,
        "order_items": items,
        "order_reviews": reviews,
        "products": products,
        "product_category_name_translation": translation,
        "comtrade_trade_flows": comtrade,
        "world_bank_lpi": lpi,
    }
    for name, df in tables.items():
        conn.register("_df", df)
        conn.execute(f"CREATE TABLE bronze.{name} AS SELECT * FROM _df")
        conn.unregister("_df")
        n = conn.execute(f"SELECT count(*) FROM bronze.{name}").fetchone()[0]
        print(f"  bronze.{name}: {n} rows")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a synthetic bronze-layer DuckDB fixture for CI")
    parser.add_argument("--db", required=True, help="Path to write the fixture DuckDB file")
    args = parser.parse_args()

    print(f"Building CI fixture database -> {args.db}")
    build_fixture_db(args.db)
    print("CI fixture database ready.")


if __name__ == "__main__":
    main()
