# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Bronze ingestion (run from project root)
python -m ingestion.load_bronze
python -m ingestion.load_bronze --db path/to/custom.duckdb --raw-dir path/to/csvs

# dbt (run from dbt/ directory)
cd dbt
dbt run                            # all models
dbt run --select silver            # silver layer only
dbt run --select gold              # gold layer only
dbt test
dbt docs generate && dbt docs serve

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

- **Olist** (`ingestion/load_bronze.py`): Downloads 9 CSVs from `data/raw/` into bronze. The `OLIST_TABLES` dict maps filenames to table names. Each load is a full replace (`DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT`).
- **UN Comtrade** (`ingestion/comtrade.py`): Fetches trade flow records via REST API → `bronze.comtrade_trade_flows`.
- **World Bank LPI** (`ingestion/world_bank_lpi.py`): Fetches 7 LPI indicators for all countries → `bronze.world_bank_lpi`.

### API Layer (`api/`)

FastAPI app in `api/main.py`. Routers:
- `api/routers/suppliers.py` — queries `gold.gold_supplier_performance`
- `api/routers/decisions.py` — proxies questions to the LangChain agent

All DuckDB connections in the API are opened read-only per request (no connection pool — DuckDB is embedded).

### AI Agent (`agent/decision_agent.py`)

LangChain `create_openai_functions_agent` with two tools:
- `run_sql` — executes any read-only SQL against DuckDB and returns results as text
- `list_tables` — introspects a schema to discover available tables

The agent is stateless (rebuilt per request). It is told to prefer gold → silver → bronze in its query strategy.

### Dashboard (`dashboard/app.py`)

Single-file Streamlit app. Connects to DuckDB directly (read-only) for charts; calls the FastAPI `/decisions/ask` endpoint for the AI assistant widget.

### dbt Configuration

- `dbt/profiles.yml` reads `DUCKDB_PATH` from env. Run `dbt` from the `dbt/` subdirectory so relative paths resolve correctly.
- Bronze tables are declared as `sources` in `dbt/models/silver/sources.yml` — add new bronze tables there before referencing them in models.
- Silver models use `{{ source('bronze', 'table_name') }}`; gold models can use both `{{ source(...) }}` and `{{ ref('silver_model') }}`.

## Key Conventions

- `data/raw/` is gitignored — Olist CSVs must be downloaded separately from Kaggle.
- `data/duckdb/*.duckdb` is gitignored — the database is a build artifact regenerated from ingestion + dbt.
- The `.env` file is gitignored; `.env.example` documents all required variables.
- Bronze ingestion scripts are idempotent (full replace on every run).
