# Supply Chain Decision Engine

An end-to-end supply chain analytics platform — medallion lakehouse architecture on DuckDB, dbt transformations, and a FastAPI service — for global supply chain risk and trade-concentration analysis.

> **Studying this project for an interview?** Read [`DECISIONS.md`](DECISIONS.md) —
> it documents every non-obvious engineering choice (why this metric formula and not
> another, what alternatives were rejected, and the honest limitations) phase by phase.

## Data Sources

| Source | Description |
|--------|-------------|
| [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Orders, sellers, products, reviews, payments |
| [UN Comtrade API](https://comtradeapi.un.org/) | Global trade flow data |
| [World Bank LPI](https://lpi.worldbank.org/) | Logistics Performance Index by country |

## Architecture

```
data/raw/ (Olist CSVs)          UN Comtrade API                    World Bank LPI API
    ↓  ingestion/load_bronze.py     ↓  ingestion/comtrade_pipeline.py   ↓  ingestion/world_bank_lpi.py
    |                                (weekly via dags/comtrade_weekly_dag.py)
DuckDB bronze schema  (raw tables + _source_file, _loaded_at metadata; Comtrade
                        upserted/deduped on a natural key — see ingestion/comtrade.py)
    ↓  dbt silver models
DuckDB silver schema  (cleaned, typed, natural grain — no aggregation):
    silver_orders, silver_order_items, silver_sellers
    silver_comtrade_trade_flows (world-aggregate), silver_comtrade_partner_flows (per-country)
    silver_world_bank_lpi
    ↓  dbt gold models
DuckDB gold schema:
    gold_supplier_scorecard          — per-seller reliability score, lead-time variability, risk tier
    gold_concentration_risk          — per-seller revenue share + HHI (single-supplier dependency)
    gold_geo_concentration           — per-state revenue share (geographic dependency)
    gold_sourcing_cost_drivers       — per-category freight burden analysis
    gold_country_logistics_scorecard — World Bank LPI, pivoted wide, one row per country
    gold_trade_balance               — Comtrade imports/exports/balance per reporter+period
    gold_trade_concentration         — per-product source-country HHI (which countries dominate supply)
    gold_trade_concentration_shift   — period-over-period change in the above
    gold_macro_context               — Brazil-specific macro backdrop (LPI + trade balance)
    gold_risk_score_validation       — out-of-sample backtest: does risk_tier predict future late deliveries?
    gold_executive_summary           — single-row portfolio KPI rollup
    ↓
FastAPI  (/suppliers — the CI-tested, verified path)
Streamlit dashboard  (charts direct from DuckDB, CI-tested; Trade-Partner Concentration
                       section reflects whatever the weekly DAG last landed)
```

An LLM-based decision agent (`agent/decision_agent.py`, a direct Anthropic SDK
tool-calling loop against the gold layer) and a corresponding `/decisions/ask`
endpoint and dashboard widget also exist in this repo. They're not covered by CI
(`tests/test_agent.py` requires a live `ANTHROPIC_API_KEY` that isn't configured as a
secret anywhere) and have never been verified end-to-end against a real Claude
response — see `DECISIONS.md`, Phase 4 and Phase 7, for exactly what was and wasn't
checked. Treat that code as present, not as a proven capability.

On Streamlit Community Cloud, `streamlit_app.py` bootstraps a synthetic database via
`data/sample_data.py` instead of running dbt (Cloud can't run the real Kaggle-CSV
pipeline) — see `DECISIONS.md` for why that duplication exists, and Phase 6 for why
that means the *public* Cloud dashboard does not actually receive the weekly
Comtrade pipeline's live output (it shows a static synthetic example instead) —
only a self-hosted deployment sharing the same DuckDB file as the Airflow DAG does.

## Quickstart

```bash
# 1. Install dependencies (full local stack — dbt, FastAPI, the decision agent, tests)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# 2. Configure environment
cp .env.example .env
# edit .env — set ANTHROPIC_API_KEY, COMTRADE_API_KEY, etc.

# 3. Download Olist CSVs from Kaggle and place them in data/raw/

# 4. Load bronze layer
python -m ingestion.load_bronze

# 4b. Optional: UN Comtrade (world-aggregate) + World Bank LPI (feeds gold_macro_context)
#     — no API key needed for either; --countries scopes the World Bank pull to
#     minutes instead of a slow all-countries run (see ingestion/world_bank_lpi.py)
python -m ingestion.comtrade
python -m ingestion.world_bank_lpi --countries BRA USA CHN DEU ARG

# 4c. Optional: UN Comtrade partner-country breakdown (feeds gold_trade_concentration)
#     — the pipeline the weekly Airflow DAG runs; safe to re-run (dedup on natural key)
python -m ingestion.comtrade_pipeline

# 5. Run dbt silver + gold transformations (and run the test suite)
cd dbt && dbt build && cd ..

# 6. Print the pipeline instrumentation report (rows/tables/volume + flagging rates)
python -m scripts.pipeline_report

# 7. Start API
uvicorn api.main:app --reload

# 8. Start dashboard (separate terminal)
streamlit run dashboard/app.py
```

## Orchestration (Airflow)

`dags/comtrade_weekly_dag.py` runs the Comtrade partner-breakdown pull on a weekly
schedule, then refreshes dbt. Run it once manually without Airflow at all:

```bash
python -m ingestion.comtrade_pipeline                      # default: Brazil + Argentina, 3 products
python -m ingestion.comtrade_pipeline --reporters 76 --commodities 85 33
```

For the real scheduled version, see [Docker](#docker) below —
`docker-compose.airflow.yml` runs a self-hosted Airflow (scheduler + API server +
Postgres). The DAG's actual task logic was validated by executing it for real
against a temporary local Airflow 3.3.0 install (`airflow tasks test`, both tasks,
hitting the live Comtrade API and running a real `dbt build`) — not just parsed.
See `DECISIONS.md`, Phase 6, for exactly what that proved.

## Docker

Two images, one per service — `docker/Dockerfile.api` (minimal runtime deps, this is
also what's deployed to AWS, see below) and `docker/Dockerfile.dashboard` (reuses the
same slim `requirements.txt` as the Streamlit Cloud deployment):

```bash
cd docker
docker compose up --build
```

API: http://localhost:8000  
Dashboard: http://localhost:8501  
API docs: http://localhost:8000/docs

A third compose file, `docker/docker-compose.airflow.yml`, runs self-hosted Airflow
(see [Orchestration](#orchestration-airflow) above). Run it **alongside** the compose
file above, on the same host — both bind-mount the same `../data` directory, so the
weekly DAG's pulls and dbt refreshes actually reach the dashboard started here, live:

```bash
cd docker
docker compose -f docker-compose.airflow.yml up --build   # Airflow UI: http://localhost:8080 (admin/admin)
```

## Cloud Deployment

The data layer (bronze/silver/gold as Parquet, plus a DuckDB snapshot) can be exported
to S3, and the same containerized FastAPI image deployed unmodified to either — or
both — of two clouds:

- **AWS** ([`aws/README.md`](aws/README.md)) — App Runner, backed by the S3 data layer.
- **GCP** ([`gcp/README.md`](gcp/README.md)) — Cloud Run, reading from the *same* S3
  bucket rather than a separate GCS copy, to prove the compute layer is genuinely
  portable rather than maintaining two parallel data copies. See `DECISIONS.md`
  (Phase 4) for the full reasoning and its tradeoffs.

Both are validated (`terraform fmt`/`init`/`validate`, schema-checked against the real
AWS/GCP provider) but **not applied** in this repo's history — no cloud credentials
were available when they were built. Deploying either creates real, billable
resources; run `terraform apply` yourself. This keeps the local/Docker/Streamlit-Cloud
setups above completely unaffected — cloud deployment is an additional target, not a
replacement for any of them.

## Development

```bash
# Lint
ruff check .

# Tests
pytest tests/
pytest tests/test_load_bronze.py   # single file

# dbt
cd dbt
dbt run --select silver          # run only silver models
dbt test
dbt docs generate && dbt docs serve

# Export the data layer (Parquet + DuckDB snapshot) to a local dir or S3
python -m scripts.export_to_s3 --dest /tmp/export_test   # local, no AWS needed
python -m scripts.export_to_s3 --dest s3://<bucket>      # real upload, needs AWS creds
```
