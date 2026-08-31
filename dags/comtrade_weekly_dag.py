"""
Weekly Comtrade trade-flow pull + dbt refresh.

Deliberately thin: this file is orchestration glue only. All actual pull/pagination/
rate-limit/dedup logic lives in ingestion/comtrade_pipeline.py and
ingestion/comtrade.py, as plain, independently-testable Python (see
tests/test_comtrade_pipeline.py) — none of it depends on Airflow being installed or
running. This DAG just calls that code on a schedule and chains a dbt refresh after
it. See DECISIONS.md, Phase 6, for why this split matters and exactly how this DAG
was validated (a real, temporary local Airflow 3.3.0 install, both tasks actually
executed via `airflow tasks test` — not just parsed).

Two tasks:
  1. pull_comtrade_data   — ingestion.comtrade_pipeline.run_weekly_pull()
  2. refresh_dbt_models   — `dbt build`, so silver/gold reflect whatever pull #1 landed

Deploy: see docker/docker-compose.airflow.yml for a local/self-hosted Airflow, or
DECISIONS.md for the managed-service (MWAA/Cloud Composer) alternative and why this
project scoped to the former instead of building full Terraform for the latter.
"""

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# airflow.decorators (not the newer airflow.sdk path) deliberately: this import
# works unchanged on both Airflow 2.x (the version most real deployments run today)
# and 3.x, whereas airflow.sdk is 3.x-only and doesn't exist on 2.x at all. On 3.x
# this prints a deprecation warning pointing at airflow.sdk — a cosmetic issue, not
# a functional one, and the safer tradeoff than an ImportError on 2.x. See
# DECISIONS.md, Phase 6, for how this was actually validated (Airflow 3.3.0, the
# only release installable against this project's Python 3.13 environment).
from airflow.decorators import dag, task

# Airflow imports this file directly (not as part of the `ingestion` package's own
# test/run context), so the repo root needs to be on sys.path for
# `from ingestion.comtrade_pipeline import run_weekly_pull` to resolve. Airflow's
# DAG folder is whatever's mounted at /opt/airflow/dags (see
# docker-compose.airflow.yml) — REPO_ROOT assumes this file's parent's parent is the
# repo root, true both locally (dags/ at repo root) and in the container (repo
# mounted at /opt/airflow, dags/ symlinked or copied under it).
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

log = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(REPO_ROOT / "data" / "duckdb" / "supply_chain.duckdb"))


default_args = {
    "owner": "supply-chain-decision-engine",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Airflow-level retry handles a task crashing outright (OOM, an unhandled
    # exception, the container restarting mid-run) — a different failure class from
    # the per-HTTP-call retry-with-backoff already inside fetch_trade_flows()
    # (comtrade.py) and _get_with_retry() (world_bank_lpi.py), which handle a single
    # request failing transiently. Both layers exist because they catch different
    # things; neither alone is enough.
}


@dag(
    dag_id="comtrade_weekly_pull",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,  # Comtrade's annual data doesn't retroactively need a run for
                     # every week since 2026-01-01 — backfilling weekly runs against
                     # a source that updates roughly annually would just be
                     # hundreds of redundant, rate-limit-risking calls for
                     # identical data. See DECISIONS.md, Phase 6, "why weekly."
    max_active_runs=1,  # DuckDB is a single embedded file, not a client-server
                         # database — two pipeline runs writing concurrently would
                         # race on the same file. The dedup logic in upsert_bronze()
                         # makes a SEQUENTIAL retry safe; it does not make
                         # CONCURRENT writes safe, which is a different problem.
    default_args=default_args,
    tags=["comtrade", "trade-concentration"],
)
def comtrade_weekly_pull():
    @task()
    def pull_comtrade_data() -> dict:
        from ingestion.comtrade_pipeline import run_weekly_pull

        summary = run_weekly_pull(db_path=DUCKDB_PATH)
        if summary["truncated"]:
            log.warning(
                "This run's partner breakdown was truncated for: %s — see "
                "DECISIONS.md, Phase 6, on the free preview API's ~500-row cap.",
                summary["truncated"],
            )
        return summary

    @task()
    def refresh_dbt_models(pull_summary: dict) -> None:
        """Rebuild silver/gold so gold_trade_concentration /
        gold_trade_concentration_shift reflect what pull_comtrade_data just landed.
        Runs the exact `dbt build` command documented in CLAUDE.md/README for local
        development — no separate Airflow-specific dbt invocation to keep in sync
        with how a human runs the same build."""
        log.info("Refreshing dbt models after Comtrade pull: %s", pull_summary)
        result = subprocess.run(
            ["dbt", "build"],
            cwd=str(REPO_ROOT / "dbt"),
            env={**os.environ, "DUCKDB_PATH": DUCKDB_PATH},
            capture_output=True,
            text=True,
            check=False,
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"dbt build failed with exit code {result.returncode}")

    refresh_dbt_models(pull_comtrade_data())


comtrade_weekly_pull()
