"""
Load Olist Brazilian ecommerce CSVs from data/raw/ into DuckDB bronze schema.

Download the dataset from:
  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
Place all CSV files under data/raw/ before running.

Usage:
    python -m ingestion.load_bronze
    python -m ingestion.load_bronze --db path/to/custom.duckdb
    python -m ingestion.load_bronze --raw-dir path/to/csvs
"""

import argparse
import logging
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Olist CSV filename → bronze table name
OLIST_TABLES: dict[str, str] = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
}

BRONZE_SCHEMA = "bronze"


def get_db_path() -> str:
    return os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    log.info("Connected to DuckDB at %s", db_path)
    return conn


def bootstrap_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    log.info("Schema '%s' ready", BRONZE_SCHEMA)


def load_table(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    table_name: str,
) -> int:
    """Load a single CSV into bronze.<table_name>, replacing any existing data."""
    qualified = f"{BRONZE_SCHEMA}.{table_name}"
    conn.execute(f"DROP TABLE IF EXISTS {qualified}")
    conn.execute(
        f"""
        CREATE TABLE {qualified} AS
        SELECT
            *,
            '{csv_path.name}'   AS _source_file,
            current_timestamp   AS _loaded_at
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        """
    )
    row_count: int = conn.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    log.info("  %-55s → %s  (%,d rows)", csv_path.name, qualified, row_count)
    return row_count


def load_all(raw_dir: Path, db_path: str) -> dict[str, int]:
    conn = connect(db_path)
    bootstrap_schema(conn)

    stats: dict[str, int] = {}
    missing: list[str] = []

    for filename, table_name in OLIST_TABLES.items():
        csv_path = raw_dir / filename
        if not csv_path.exists():
            log.warning("  Missing: %s — skipping", csv_path)
            missing.append(filename)
            continue
        stats[table_name] = load_table(conn, csv_path, table_name)

    conn.close()

    log.info("")
    log.info("Bronze load complete: %d tables loaded, %d skipped.", len(stats), len(missing))
    if missing:
        log.warning("Missing files: %s", missing)

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Olist CSVs into DuckDB bronze schema")
    parser.add_argument("--db", default=get_db_path(), help="Path to DuckDB file")
    parser.add_argument("--raw-dir", default="data/raw", help="Directory containing Olist CSVs")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    if not raw_dir.exists():
        log.error("Raw data directory not found: %s", raw_dir)
        raise SystemExit(1)

    load_all(raw_dir=raw_dir, db_path=args.db)


if __name__ == "__main__":
    main()
