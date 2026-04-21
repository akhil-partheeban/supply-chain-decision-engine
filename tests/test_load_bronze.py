"""Integration tests for bronze ingestion — requires real CSV files or temp fixtures."""

import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ingestion.load_bronze import bootstrap_schema, load_all, load_table, BRONZE_SCHEMA


@pytest.fixture()
def tmp_db(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    bootstrap_schema(conn)
    conn.close()
    return db_path


@pytest.fixture()
def sample_csv(tmp_path) -> Path:
    df = pd.DataFrame(
        {
            "order_id": ["a1", "a2"],
            "customer_id": ["c1", "c2"],
            "order_status": ["delivered", "shipped"],
            "order_purchase_timestamp": ["2023-01-01 10:00:00", "2023-01-02 11:00:00"],
            "order_estimated_delivery_date": ["2023-01-10 00:00:00", "2023-01-11 00:00:00"],
            "order_delivered_customer_date": ["2023-01-09 00:00:00", None],
        }
    )
    csv_path = tmp_path / "olist_orders_dataset.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_load_table(tmp_db, sample_csv):
    conn = duckdb.connect(tmp_db)
    bootstrap_schema(conn)
    n = load_table(conn, sample_csv, "orders")
    assert n == 2
    result = conn.execute(f"SELECT count(*) FROM {BRONZE_SCHEMA}.orders").fetchone()[0]
    assert result == 2
    conn.close()


def test_load_all_missing_files(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    db_path = str(tmp_path / "test.duckdb")
    stats = load_all(raw_dir=raw_dir, db_path=db_path)
    assert stats == {}
