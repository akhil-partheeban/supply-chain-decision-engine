# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (full local dev stack — dbt, FastAPI, the decision agent, tests)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
# requirements.txt (root) is a separate, intentionally slim file used only by the
# Streamlit Community Cloud deployment — see DECISIONS.md.
# requirements-api.txt / requirements-airflow.txt are narrower still, for their
# respective containers (docker/Dockerfile.api, docker/Dockerfile.airflow) — not
# needed for local dev unless reproducing that exact container's dependency set.

# Bronze ingestion (run from project root)
python -m ingestion.load_bronze
python -m ingestion.load_bronze --db path/to/custom.duckdb --raw-dir path/to/csvs

# UN Comtrade + World Bank LPI ingestion (optional — feeds gold_macro_context)
python -m ingestion.comtrade                          # free preview API, no key needed
python -m ingestion.comtrade --reporter 76 --period 2022
python -m ingestion.world_bank_lpi --countries BRA USA CHN   # scoped, minutes not tens of minutes
python -m ingestion.world_bank_lpi                           # all countries — slow, see module docstring

# UN Comtrade partner-country breakdown (feeds gold_trade_concentration) — this is
# what dags/comtrade_weekly_dag.py runs weekly; safe to re-run (upsert/dedup, not
# full-replace — see ingestion/comtrade.py's upsert_bronze())
python -m ingestion.comtrade_pipeline
python -m ingestion.comtrade_pipeline --reporters 76 32 --commodities 85 33 94

# dbt (run from dbt/ directory)
cd dbt
dbt run                            # all models
dbt run --select silver            # silver layer only
dbt run --select gold              # gold layer only
dbt test
dbt docs generate && dbt docs serve

# Pipeline instrumentation report — rows/tables/volume + gold-layer flagging rates
# (run after ingestion + dbt build; writes reports/pipeline_metrics.json)
python -m scripts.pipeline_report

# API (from project root)
uvicorn api.main:app --reload --port 8000

# Dashboard
streamlit run dashboard/app.py

# Lint
ruff check .
ruff check . --fix

# Tests
pytest tests/
pytest tests/test_load_bronze.py   # single file
pytest -k test_load_table          # single test
```

## Architecture

### Medallion Lakehouse (DuckDB)

All data lives in a single DuckDB file (`data/duckdb/supply_chain.duckdb`) with three schemas:

| Schema | Populated by | Contents |
|--------|-------------|----------|
| `bronze` | `ingestion/` Python scripts | Raw tables, exactly as ingested; every row gets `_source_file` and `_loaded_at` metadata columns |
| `silver` | dbt models in `dbt/models/silver/` | Cleaned, typed, enriched; timestamps parsed, nulls handled |
| `gold` | dbt models in `dbt/models/gold/` | Aggregated KPIs and scorecards for direct consumption by the API and dashboard |

### Data Sources

- **Olist** (`ingestion/load_bronze.py`): Loads 9 CSVs from `data/raw/` into bronze. The `OLIST_TABLES` dict maps filenames to table names. Each load is a full replace (`DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT`).
- **UN Comtrade — world aggregate** (`ingestion/comtrade.py`, `main()`/CLI): Fetches world-aggregate trade flow records → `bronze.comtrade_trade_flows` (full-replace via `load_bronze()`). Uses the free `/public/v1/preview` endpoint by default (no key required, capped/rate-limited); switches to the full `/data/v1/get` endpoint automatically once a real `COMTRADE_API_KEY` is set. typeCode/freqCode/clCode are URL path segments, not query params — see the module docstring before changing the URL construction.
- **UN Comtrade — partner-country breakdown** (`ingestion/comtrade_pipeline.py`, run weekly by `dags/comtrade_weekly_dag.py`): Fetches per-partner-country trade values for a configurable set of reporters/HS commodity codes → the same `bronze.comtrade_trade_flows` table, but via `upsert_bronze()` (delete-then-insert dedup on a natural key — see `COMTRADE_NATURAL_KEY` in `ingestion/comtrade.py`), **not** a full replace, since this pull accumulates a time series across recurring runs instead of replacing itself each time. `partner=""` (empty string, Comtrade's own convention) is what triggers the per-partner breakdown instead of a world-aggregate total.
- **World Bank LPI** (`ingestion/world_bank_lpi.py`): Fetches 7 LPI indicators for the requested countries (`--countries`, default all) → `bronze.world_bank_lpi`. No API key needed; a full all-countries pull is genuinely slow (see module docstring for measured timing) — scope with `--countries` for a fast iteration loop.

The world-aggregate Comtrade pull + World Bank LPI feed `dbt/models/gold/gold_macro_context.sql`, pairing Brazil's national logistics/trade backdrop with the Olist-derived supplier scorecards. The partner-breakdown pull feeds `gold_trade_concentration.sql` / `gold_trade_concentration_shift.sql` — per-product source-country concentration (HHI) and how it changes between pulls. See DECISIONS.md, Phase 6, for the full pipeline design (rate limits, pagination/truncation handling, the Airflow DAG, and what does/doesn't stay live end to end).

### Orchestration (`dags/`)

`dags/comtrade_weekly_dag.py` — an Airflow DAG (`@dag`/`@task` TaskFlow API, `schedule="@weekly"`, `catchup=False`, `max_active_runs=1`) with two tasks: `pull_comtrade_data` (calls `ingestion.comtrade_pipeline.run_weekly_pull()`) then `refresh_dbt_models` (`dbt build` as a subprocess). All actual logic lives in `ingestion/`, fully testable without Airflow installed — the DAG file is orchestration glue only. Deploy via `docker/docker-compose.airflow.yml` (self-hosted, LocalExecutor + Postgres) alongside `docker-compose.yml` on the same host, sharing the same `data/` volume, for the pipeline's output to actually reach a running dashboard.

### API Layer (`api/`)

FastAPI app in `api/main.py`. Routers:
- `api/routers/suppliers.py` — queries `gold.gold_supplier_scorecard`
- `api/routers/decisions.py` — proxies questions to the decision agent

All DuckDB connections in the API are opened read-only per request (no connection pool — DuckDB is embedded).

### AI Agent (`agent/decision_agent.py`)

Direct Anthropic SDK tool-calling loop (**not** LangChain, despite this file's name and some legacy comments elsewhere — see DECISIONS.md, Phase 4, for why that mismatch existed and was corrected) with two tools:
- `run_sql` — executes any read-only SQL against DuckDB and returns results as text
- `get_executive_summary` — returns the formatted single-row `gold.gold_executive_summary` KPI rollup

The agent is stateless (rebuilt per request). Its system prompt is told to prefer gold → silver → bronze in its query strategy.

### Dashboard (`dashboard/app.py`)

Single-file Streamlit app. Connects to DuckDB directly (read-only) for charts; the AI assistant widget calls the FastAPI `/decisions/ask` endpoint (`API_BASE_URL` env var, default `http://localhost:8000`) over HTTP.

### dbt Configuration

- `dbt/profiles.yml` reads `DUCKDB_PATH` from env. Run `dbt` from the `dbt/` subdirectory so relative paths resolve correctly.
- Bronze tables are declared as `sources` in `dbt/models/silver/sources.yml` — add new bronze tables there before referencing them in models.
- Silver models use `{{ source('bronze', 'table_name') }}`; gold models can use both `{{ source(...) }}` and `{{ ref('silver_model') }}`.

## Key Conventions

- `data/raw/` is gitignored — Olist CSVs must be downloaded separately from Kaggle.
- `data/duckdb/*.duckdb` is gitignored — the database is a build artifact regenerated from ingestion + dbt.
- The `.env` file is gitignored; `.env.example` documents all required variables.
- Bronze ingestion is idempotent, by one of two mechanisms depending on the script: full replace on every run (Olist, World Bank LPI, and Comtrade's world-aggregate CLI — safe because each run is meant to fully represent "the current state"), or upsert/dedup on a natural key (Comtrade's partner-breakdown pipeline, `ingestion/comtrade.py`'s `upsert_bronze()` — necessary because this pull is meant to accumulate a time series across recurring runs, where a full replace would destroy prior periods' history on every run).
