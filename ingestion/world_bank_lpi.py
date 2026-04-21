"""
Ingest World Bank Logistics Performance Index (LPI) data into DuckDB bronze schema.

LPI indicator codes:
  LP.LPI.OVRL.XQ  - Overall LPI score
  LP.LPI.CUST.XQ  - Customs
  LP.LPI.INFR.XQ  - Infrastructure
  LP.LPI.ITRN.XQ  - International shipments
  LP.LPI.LOGS.XQ  - Logistics quality and competence
  LP.LPI.TRAC.XQ  - Tracking & tracing
  LP.LPI.TIME.XQ  - Timeliness

API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898581
"""

import logging
import os

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

BASE_URL = os.getenv("WORLD_BANK_BASE_URL", "https://api.worldbank.org/v2")
BRONZE_SCHEMA = "bronze"

LPI_INDICATORS = [
    "LP.LPI.OVRL.XQ",
    "LP.LPI.CUST.XQ",
    "LP.LPI.INFR.XQ",
    "LP.LPI.ITRN.XQ",
    "LP.LPI.LOGS.XQ",
    "LP.LPI.TRAC.XQ",
    "LP.LPI.TIME.XQ",
]


def fetch_indicator(indicator: str, country: str = "all", per_page: int = 1000) -> list[dict]:
    """Fetch all pages for a single World Bank indicator."""
    url = f"{BASE_URL}/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": per_page, "mrv": 10}
    records: list[dict] = []
    page = 1
    while True:
        params["page"] = page
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        # payload is [metadata, data]
        if len(payload) < 2 or not payload[1]:
            break
        for row in payload[1]:
            records.append(
                {
                    "country_code": row["countryiso3code"],
                    "country_name": row["country"]["value"],
                    "indicator_code": row["indicator"]["id"],
                    "indicator_name": row["indicator"]["value"],
                    "year": row["date"],
                    "value": row["value"],
                }
            )
        total_pages = payload[0].get("pages", 1)
        if page >= total_pages:
            break
        page += 1
    return records


def load_bronze(conn: duckdb.DuckDBPyConnection, table: str = "world_bank_lpi") -> int:
    import pandas as pd

    all_records: list[dict] = []
    for indicator in LPI_INDICATORS:
        log.info("Fetching WB indicator: %s", indicator)
        all_records.extend(fetch_indicator(indicator))

    if not all_records:
        log.warning("No LPI records fetched")
        return 0

    df = pd.DataFrame(all_records)
    df["_loaded_at"] = pd.Timestamp.utcnow()
    qualified = f"{BRONZE_SCHEMA}.{table}"
    conn.execute(f"DROP TABLE IF EXISTS {qualified}")
    conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM df")
    row_count = conn.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    log.info("Loaded %d rows → %s", row_count, qualified)
    return row_count
