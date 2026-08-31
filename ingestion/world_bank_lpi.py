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

No API key required — this is a fully public API. Fetching all 7 indicators for
every country (country="all") is genuinely slow: a single all-countries page at
per_page=1000 measured at 30-45 seconds in this project's own testing, and an
indicator can span multiple pages — see DECISIONS.md, Phase 4, for the full timing
and why REQUEST_TIMEOUT_SECONDS is 60, not requests' shorter conventional defaults.

Usage:
    python -m ingestion.world_bank_lpi
    python -m ingestion.world_bank_lpi --countries BRA USA CHN   # faster, scoped run
    python -m ingestion.world_bank_lpi --db path/to/custom.duckdb
"""

import argparse
import logging
import os
import time

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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

REQUEST_TIMEOUT_SECONDS = 90
MAX_ATTEMPTS = 3


def _get_with_retry(url: str, params: dict) -> requests.Response:
    """The World Bank API is measurably slow and occasionally times out even for a
    small, scoped request (observed directly during this project's own testing, not
    a hypothetical) — a couple of retries makes a multi-page, multi-indicator pull
    survive one transient slow response instead of failing the whole run."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < MAX_ATTEMPTS:
                log.warning("Request failed (%s) — retrying (attempt %d/%d)", exc, attempt, MAX_ATTEMPTS)
                time.sleep(3 * attempt)
    raise last_exc


def fetch_indicator(indicator: str, country: str = "all", per_page: int = 1000) -> list[dict]:
    """Fetch all pages for a single World Bank indicator."""
    url = f"{BASE_URL}/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": per_page, "mrv": 10}
    records: list[dict] = []
    page = 1
    while True:
        params["page"] = page
        resp = _get_with_retry(url, params)
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
        log.info("  %s page %d/%d -> %d rows so far", indicator, page, total_pages, len(records))
        if page >= total_pages:
            break
        page += 1
    return records


def load_bronze(
    conn: duckdb.DuckDBPyConnection,
    country: str = "all",
    table: str = "world_bank_lpi",
) -> int:
    import pandas as pd

    all_records: list[dict] = []
    for indicator in LPI_INDICATORS:
        log.info("Fetching WB indicator: %s (country=%s)", indicator, country)
        all_records.extend(fetch_indicator(indicator, country=country))

    if not all_records:
        log.warning("No LPI records fetched")
        return 0

    df = pd.DataFrame(all_records)
    df["_loaded_at"] = pd.Timestamp.now("UTC")
    qualified = f"{BRONZE_SCHEMA}.{table}"
    conn.execute(f"DROP TABLE IF EXISTS {qualified}")
    conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM df")
    row_count = conn.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    log.info("Loaded %s rows -> %s", f"{row_count:,}", qualified)
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest World Bank LPI data into DuckDB bronze schema")
    parser.add_argument("--db", default=os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb"))
    parser.add_argument(
        "--countries", nargs="+", default=None,
        help="ISO3 country codes to scope the pull (default: all countries — slow, see module docstring)",
    )
    args = parser.parse_args()

    country_param = ";".join(args.countries) if args.countries else "all"

    conn = duckdb.connect(args.db)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    load_bronze(conn, country=country_param)
    conn.close()


if __name__ == "__main__":
    main()
