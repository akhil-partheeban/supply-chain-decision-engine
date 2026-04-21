"""
Ingest UN Comtrade trade flow data into DuckDB bronze schema.

API docs: https://comtradeapi.un.org/
Requires COMTRADE_API_KEY in .env.
"""

import logging
import os

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

BASE_URL = os.getenv("COMTRADE_BASE_URL", "https://comtradeapi.un.org/data/v1")
API_KEY = os.getenv("COMTRADE_API_KEY", "")
BRONZE_SCHEMA = "bronze"


def fetch_trade_flows(
    reporter: str,
    partner: str = "0",  # "0" = world
    commodity_code: str = "TOTAL",
    period: str = "2023",
    trade_flow: str = "M",  # M=imports, X=exports
) -> list[dict]:
    """Fetch a page of trade flow records from Comtrade v1 API."""
    url = f"{BASE_URL}/get"
    params = {
        "typeCode": "C",
        "freqCode": "A",
        "clCode": "HS",
        "period": period,
        "reporterCode": reporter,
        "cmdCode": commodity_code,
        "flowCode": trade_flow,
        "partnerCode": partner,
        "partner2Code": "0",
        "maxRecords": 500,
        "format": "JSON",
        "countOnly": False,
        "includeDesc": True,
        "subscription-key": API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def load_bronze(conn: duckdb.DuckDBPyConnection, records: list[dict], table: str = "comtrade_trade_flows") -> int:
    if not records:
        log.warning("No records to load for %s", table)
        return 0
    import pandas as pd

    df = pd.DataFrame(records)
    df["_loaded_at"] = pd.Timestamp.utcnow()
    qualified = f"{BRONZE_SCHEMA}.{table}"
    conn.execute(f"DROP TABLE IF EXISTS {qualified}")
    conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM df")
    row_count = conn.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    log.info("Loaded %d rows → %s", row_count, qualified)
    return row_count
