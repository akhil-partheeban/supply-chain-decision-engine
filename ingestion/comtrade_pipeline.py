"""
Weekly Comtrade pull orchestration: the configurable, multi-reporter/multi-product
pull that lands partner-level trade-flow breakdowns into bronze with dedup.

Deliberately plain functions, not Airflow tasks — dags/comtrade_weekly_dag.py is a
thin wrapper that imports and calls run_weekly_pull() from a PythonOperator. This
keeps the actual pull/pagination/rate-limit logic fully testable with `pytest` and
mocked HTTP (see tests/test_comtrade_pipeline.py), independent of whether Airflow
itself is installed or running — the same separation of "business logic" from
"orchestration glue" this project has used since Phase 4's ingestion CLIs.

Usage:
    python -m ingestion.comtrade_pipeline
    python -m ingestion.comtrade_pipeline --reporters 76 32 --commodities 85 33
"""

import argparse
import logging
import os
import time
from datetime import datetime, timezone

import duckdb
from dotenv import load_dotenv

from ingestion.comtrade import fetch_trade_flows, upsert_bronze

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Brazil (this project's home market) + Argentina (a real, regionally-adjacent
# trading partner already used as a comparison point in gold_country_logistics_scorecard,
# Phase 4) — a small default set, not an arbitrary one, chosen to demonstrate genuine
# multi-reporter configurability without multiplying API calls for its own sake.
DEFAULT_REPORTERS = ["76", "32"]

# 2-digit HS chapters chosen to line up with Olist's own top product categories
# (Phase 1's gold_sourcing_cost_drivers uses these same category names) so the
# macro trade-concentration lens and the transaction-level cost-driver lens are
# looking at recognizably the same product families, not unrelated commodities:
#   85 = electrical machinery/electronics   (Olist: "electronics")
#   33 = essential oils, cosmetics/toiletries (Olist: "health_beauty")
#   94 = furniture                           (Olist: "furniture")
# HS 85 is deliberately kept broad (a full 2-digit chapter) rather than narrowed to
# a sub-heading that would dodge the free API's ~500-record cap — this project would
# rather demonstrate truncation detection working on a real, broad category than
# hand-pick codes specifically to avoid ever exercising that code path.
DEFAULT_COMMODITIES = ["85", "33", "94"]

FLOW = "M"  # imports — "which countries dominate as a source for this product" is an import question
PREVIEW_ROW_CAP = 500  # Comtrade's free preview tier's observed hard cap; see DECISIONS.md, Phase 6
INTER_CALL_PACING_SECONDS = 1.5  # proactive spacing between calls, on top of fetch_trade_flows' own 429 retry


def detect_latest_period(reporter: str, candidate_years: list[str] | None = None) -> str:
    """Probe descending years with a cheap world-aggregate query (partner=0,
    cmdCode=TOTAL) and return the first year with any data. Dynamic on purpose —
    hardcoding a year (as Phase 4's one-off CLI pull did) means the pipeline quietly
    goes stale the day that hardcoded year is no longer "latest." A weekly-scheduled
    pipeline that never notices a new period has become available defeats the point
    of being scheduled at all.
    """
    current_year = datetime.now(timezone.utc).year
    years = candidate_years or [str(current_year - offset) for offset in range(1, 6)]

    for year in years:
        records = fetch_trade_flows(reporter=reporter, partner="0", commodity_code="TOTAL", period=year, trade_flow=FLOW)
        if records:
            log.info("Latest available period for reporter=%s: %s", reporter, year)
            return year
        time.sleep(INTER_CALL_PACING_SECONDS)

    raise RuntimeError(f"No available Comtrade period found for reporter={reporter} in candidates={years}")


def run_weekly_pull(
    db_path: str,
    reporters: list[str] | None = None,
    commodities: list[str] | None = None,
    run_id: str | None = None,
    period: str | None = None,
) -> dict:
    """Pull the configured reporters x commodities for the latest available period
    and upsert into bronze. Returns a summary dict — reporters/commodities/period
    pulled, total rows fetched and upserted, and any truncation warnings — meant to
    be logged and/or pushed to Airflow XCom by the calling task.

    period: normally left as None so detect_latest_period() decides — this is what
    the weekly DAG always does. Pass an explicit year for a manual backfill of a
    specific past period (e.g. building up the history gold_trade_concentration_shift
    needs to compute a period-over-period delta from). The DAG itself never sets this.
    """
    reporters = reporters or DEFAULT_REPORTERS
    commodities = commodities or DEFAULT_COMMODITIES
    run_id = run_id or datetime.now(timezone.utc).isoformat()

    period = period or detect_latest_period(reporters[0])

    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    all_records = []
    truncated: list[dict] = []

    for reporter in reporters:
        for commodity in commodities:
            time.sleep(INTER_CALL_PACING_SECONDS)  # proactive pacing — see module docstring
            records = fetch_trade_flows(
                reporter=reporter, partner="", commodity_code=commodity, period=period, trade_flow=FLOW,
            )
            if len(records) >= PREVIEW_ROW_CAP:
                log.warning(
                    "reporter=%s commodity=%s period=%s returned %d rows (>= the %d-row preview "
                    "cap) — this partner breakdown is likely TRUNCATED. See DECISIONS.md, Phase 6.",
                    reporter, commodity, period, len(records), PREVIEW_ROW_CAP,
                )
                truncated.append({"reporter": reporter, "commodity": commodity, "period": period, "rows": len(records)})
            all_records.extend(records)

    result = upsert_bronze(conn, all_records, run_id=run_id)
    conn.close()

    summary = {
        "run_id": run_id,
        "period": period,
        "reporters": reporters,
        "commodities": commodities,
        "fetched": result["fetched"],
        "rows_before": result["rows_before"],
        "rows_after": result["rows_after"],
        "truncated": truncated,
    }
    log.info("Weekly Comtrade pull complete: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the configurable weekly Comtrade partner-breakdown pull")
    parser.add_argument("--db", default=os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb"))
    parser.add_argument("--reporters", nargs="+", default=None, help=f"ISO numeric reporter codes (default: {DEFAULT_REPORTERS})")
    parser.add_argument("--commodities", nargs="+", default=None, help=f"HS commodity codes (default: {DEFAULT_COMMODITIES})")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    run_weekly_pull(args.db, reporters=args.reporters, commodities=args.commodities, run_id=args.run_id)


if __name__ == "__main__":
    main()
