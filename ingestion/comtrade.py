"""
Ingest UN Comtrade trade flow data into DuckDB bronze schema.

API docs: https://comtradeapi.un.org/

Two endpoint families exist, and this module supports both:
  - /public/v1/preview/{typeCode}/{freqCode}/{clCode}  — free, no subscription key,
    capped at ~500 records/query and a limited recent time window. Used by default.
  - /data/v1/get/{typeCode}/{freqCode}/{clCode}         — full historical data,
    requires a registered subscription key (COMTRADE_API_KEY). Used automatically
    once a real key is set in .env.

typeCode/freqCode/clCode are PATH segments in both API families, not query
parameters — an earlier version of this module put them in the query string instead,
which routed to a 404 on every single call. That bug was never caught because
nothing in this codebase ever called this module end-to-end (no CLI entrypoint
existed until this fix). See DECISIONS.md, Phase 4, for the full story.

Usage:
    python -m ingestion.comtrade                        # Brazil, current year, free API
    python -m ingestion.comtrade --reporter 76 --period 2022
    python -m ingestion.comtrade --db path/to/custom.duckdb
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

PREVIEW_BASE_URL = "https://comtradeapi.un.org/public/v1/preview"
DATA_BASE_URL = os.getenv("COMTRADE_BASE_URL", "https://comtradeapi.un.org/data/v1")
BRONZE_SCHEMA = "bronze"

# Brazil is this project's home market (the Olist dataset is Brazilian e-commerce) —
# its own trade flows are the macro context paired with the gold layer's Brazil
# state-level concentration metrics. See dbt/models/gold/gold_macro_context.sql.
DEFAULT_REPORTER = "76"  # ISO numeric code for Brazil


def _has_real_api_key() -> bool:
    key = os.getenv("COMTRADE_API_KEY", "")
    return bool(key) and "your_" not in key.lower() and "here" not in key.lower()


def fetch_trade_flows(
    reporter: str = DEFAULT_REPORTER,
    partner: str = "0",  # "0" = world
    commodity_code: str = "TOTAL",
    period: str = "2023",
    trade_flow: str = "M",  # M=imports, X=exports
    type_code: str = "C",
    freq_code: str = "A",
    cl_code: str = "HS",
) -> list[dict]:
    """Fetch trade flow records from Comtrade (free preview API, or the full data
    API if a real COMTRADE_API_KEY is configured).

    partner="0" (the default) returns the world-aggregate total. partner="" (empty
    string, not omitted — Comtrade's own convention) returns one row per actual
    trading partner country, which is what a concentration-by-source-country
    analysis needs. Verified against the free preview tier only; the paid data API
    is assumed to follow the same convention (both tiers share request shape) but
    this wasn't independently confirmed since no paid key was available — see
    DECISIONS.md, Phase 6.
    """
    use_preview = not _has_real_api_key()

    params = {
        "period": period,
        "reporterCode": reporter,
        "cmdCode": commodity_code,
        "flowCode": trade_flow,
        "partnerCode": partner,
        "partner2Code": "0",
        "format": "JSON",
        "includeDesc": True,
    }

    if use_preview:
        url = f"{PREVIEW_BASE_URL}/{type_code}/{freq_code}/{cl_code}"
    else:
        url = f"{DATA_BASE_URL}/get/{type_code}/{freq_code}/{cl_code}"
        params["subscription-key"] = os.getenv("COMTRADE_API_KEY", "")
        params["maxRecords"] = 500
        params["countOnly"] = False

    log.info(
        "Fetching Comtrade trade flows (%s API): reporter=%s period=%s flow=%s partner=%s",
        "preview" if use_preview else "full", reporter, period, trade_flow, partner or "(all)",
    )

    # The free preview API enforces an undocumented, tight rate limit — measured in
    # this project's own testing as inconsistent: sometimes a handful of back-to-back
    # calls succeed cleanly, sometimes the second call in quick succession gets a
    # 429. There is no documented SLA to target, so this retry is a pragmatic
    # mitigation, not a guarantee — see DECISIONS.md, Phase 6, for the full
    # characterization. Also retries transient network errors (timeouts, connection
    # resets), which a 429-only retry would miss.
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException as exc:
            if attempt == max_attempts:
                raise
            wait = 2 ** attempt
            log.warning("Request failed (%s) — retrying in %ds (attempt %d/%d)", exc, wait, attempt, max_attempts)
            time.sleep(wait)
            continue

        if resp.status_code == 429 and attempt < max_attempts:
            wait = 2 ** attempt
            log.warning("Rate limited (429) — retrying in %ds (attempt %d/%d)", wait, attempt, max_attempts)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json().get("data", [])
    return []


def load_bronze(conn: duckdb.DuckDBPyConnection, records: list[dict], table: str = "comtrade_trade_flows") -> int:
    """Full-replace load: wipe the table, load these records. Correct for a single
    ad-hoc pull (the CLI below) where "the data in this table" should mean exactly
    "the result of the last call to this script." Wrong for a recurring pull that's
    meant to accumulate a time series across runs — see upsert_bronze() for that
    case, used by the weekly Airflow DAG (dags/comtrade_weekly_dag.py)."""
    if not records:
        log.warning("No records to load for %s", table)
        return 0
    import pandas as pd

    df = pd.DataFrame(records)
    df["_loaded_at"] = pd.Timestamp.now("UTC")
    qualified = f"{BRONZE_SCHEMA}.{table}"
    conn.execute(f"DROP TABLE IF EXISTS {qualified}")
    conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM df")
    row_count = conn.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    log.info("Loaded %s rows -> %s", f"{row_count:,}", qualified)
    return row_count


# Comtrade's own grain for one customs statistic record — deliberately excludes
# every measure column (fobvalue, cifvalue, qty, netWgt, ...) and every purely
# descriptive column (reporterDesc, flowDesc, ... — often null in preview mode
# anyway). Two rows sharing all of these are the SAME underlying fact, possibly
# with revised figures; two rows differing in any of these are genuinely different
# facts. This is what makes upsert_bronze() a real dedup rather than an append.
COMTRADE_NATURAL_KEY = [
    "typeCode", "freqCode", "period", "reporterCode", "flowCode",
    "partnerCode", "partner2Code", "cmdCode", "customsCode", "motCode",
]


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [BRONZE_SCHEMA, table],
    ).fetchone()[0] > 0


def _table_row_count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return conn.execute(f"SELECT count(*) FROM {BRONZE_SCHEMA}.{table}").fetchone()[0]


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [
        r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [BRONZE_SCHEMA, table],
        ).fetchall()
    ]


def upsert_bronze(
    conn: duckdb.DuckDBPyConnection,
    records: list[dict],
    table: str = "comtrade_trade_flows",
    run_id: str | None = None,
) -> dict:
    """Land records into bronze with dedup on COMTRADE_NATURAL_KEY, for recurring or
    overlapping pulls (the weekly DAG pulling the same still-current period week
    after week, or a manual backfill that overlaps a scheduled run).

    Mechanism: delete-then-insert on the natural key, not a DuckDB native UPSERT/
    ON CONFLICT clause — this project's bronze tables have no primary key
    constraint declared (bronze is meant to mirror the source as directly as
    possible), and adding one just to unlock ON CONFLICT syntax felt like solving a
    problem this table doesn't otherwise have. Delete-then-insert needs no schema
    change, is easy to read as exactly "remove what's about to be replaced, then add
    the replacement," and is naturally idempotent: running the identical pull twice
    (a retried Airflow task, or two accidentally-overlapping manual runs) deletes and
    re-inserts the same rows, ending in the same state — not a double-write.

    Returns a small summary dict so the caller (an Airflow task) has something
    concrete to log or push to XCom, rather than a bare row count.
    """
    import pandas as pd

    rows_before = _table_row_count(conn, table)

    if not records:
        log.warning("No records to upsert for %s", table)
        return {"fetched": 0, "rows_before": rows_before, "rows_after": rows_before}

    df = pd.DataFrame(records)
    now = pd.Timestamp.now("UTC")
    df["_loaded_at"] = now
    df["_run_id"] = run_id or now.isoformat()

    qualified = f"{BRONZE_SCHEMA}.{table}"

    # A table with this name can already exist with a DIFFERENT column shape than
    # what upsert_bronze produces — e.g. one written by load_bronze() (Phase 4's
    # full-replace CLI), which never added `_run_id`, or an earlier pull whose JSON
    # response happened to include a different set of fields. Comparing row COUNT
    # alone (rows_before == 0) isn't enough to know whether an INSERT will succeed;
    # a nonempty table with a mismatched schema needs the same rebuild path as an
    # empty one. Bronze has never been treated as a system of record in this project
    # (every table here is a regenerable cache re-derived from the real source) so
    # rebuilding on a schema mismatch — logged clearly, not silent — is the correct
    # move, not data loss.
    schema_compatible = _table_exists(conn, table) and _table_columns(conn, table) == list(df.columns)
    if _table_exists(conn, table) and not schema_compatible:
        log.warning(
            "%s exists with a different column shape than this pull produces "
            "(likely written by a different code path — e.g. load_bronze()'s "
            "full-replace CLI) — rebuilding the table fresh instead of upserting.",
            qualified,
        )

    conn.register("_new_comtrade_rows", df)
    try:
        if not schema_compatible:
            conn.execute(f"DROP TABLE IF EXISTS {qualified}")
            conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM _new_comtrade_rows")
            rows_before = 0
        else:
            key_match = " AND ".join(f'existing."{col}" = new."{col}"' for col in COMTRADE_NATURAL_KEY)
            conn.execute(f"""
                DELETE FROM {qualified} AS existing
                WHERE EXISTS (SELECT 1 FROM _new_comtrade_rows AS new WHERE {key_match})
            """)
            conn.execute(f"INSERT INTO {qualified} SELECT * FROM _new_comtrade_rows")
    finally:
        conn.unregister("_new_comtrade_rows")

    rows_after = _table_row_count(conn, table)
    log.info(
        "Upserted %s fetched rows into %s -> %s rows before, %s rows after",
        f"{len(df):,}", qualified, f"{rows_before:,}", f"{rows_after:,}",
    )
    return {"fetched": len(records), "rows_before": rows_before, "rows_after": rows_after}


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest UN Comtrade trade flow data into DuckDB bronze schema")
    parser.add_argument("--db", default=os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb"))
    parser.add_argument("--reporter", default=DEFAULT_REPORTER, help="ISO numeric reporter code (default: 76, Brazil)")
    parser.add_argument("--period", default="2023", help="Year, e.g. 2023")
    parser.add_argument("--flows", nargs="+", default=["M", "X"], help="Trade flow codes to fetch (M=imports, X=exports)")
    args = parser.parse_args()

    conn = duckdb.connect(args.db)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

    all_records: list[dict] = []
    for flow in args.flows:
        all_records.extend(
            fetch_trade_flows(reporter=args.reporter, period=args.period, trade_flow=flow)
        )

    # upsert_bronze(), not load_bronze(): this CLI's world-aggregate pull
    # (cmdCode='TOTAL', partnerCode='0') writes to the SAME bronze.comtrade_trade_flows
    # table that ingestion/comtrade_pipeline.py's partner-breakdown pull also writes
    # to. load_bronze()'s DROP+CREATE full replace would silently wipe out every
    # partner-breakdown row on every run of this CLI — which is exactly what
    # happened in practice (the 'TOTAL'-commodity rows this CLI produces went
    # missing after a later comtrade_pipeline run, not the other way around, but the
    # collision is symmetric: whichever of the two runs LAST with load_bronze()
    # destroys the other's data). upsert_bronze()'s natural key includes cmdCode, so
    # 'TOTAL' rows never collide with the partner pipeline's '85'/'33'/'94' rows —
    # both pulls' data now coexists safely regardless of run order.
    result = upsert_bronze(conn, all_records, run_id=f"comtrade-cli-{args.period}")
    conn.close()

    if result["fetched"] == 0:
        log.warning(
            "Zero rows loaded — the free preview API caps results and covers only a "
            "limited recent window; try a different --period, or set a real "
            "COMTRADE_API_KEY in .env for the full data API."
        )


if __name__ == "__main__":
    main()
