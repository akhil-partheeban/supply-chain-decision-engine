# Supply Chain Decision Engine

**Objective: cut estimated Scope 3 emissions from a supplier base without raising late-delivery risk.**

An end-to-end supply chain analytics platform — medallion lakehouse architecture on DuckDB, dbt transformations, and a FastAPI service — for global supply chain risk, trade-concentration, and supplier-emissions analysis.

> **Studying this project for an interview?** Read [`DECISIONS.md`](DECISIONS.md) —
> it documents every non-obvious engineering choice (why this metric formula and not
> another, what alternatives were rejected, and the honest limitations) phase by phase.

## Data Sources

| Source | Description |
|--------|-------------|
| [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Orders, sellers, products, reviews, payments |
| [UN Comtrade API](https://comtradeapi.un.org/) | Global trade flow data |
| [World Bank LPI](https://lpi.worldbank.org/) | Logistics Performance Index by country |
| [EPA Supply Chain GHG Emission Factors](https://catalog.data.gov/dataset/supply-chain-greenhouse-gas-emission-factors-v1-3-by-naics-6) | kg CO2e per 2022 USD, by 2017 NAICS-6 commodity code (v1.3.0) — feeds the emissions layer below |

## Architecture

```
data/raw/ (Olist CSVs)          UN Comtrade API                    World Bank LPI API
    ↓  ingestion/load_bronze.py     ↓  ingestion/comtrade_pipeline.py   ↓  ingestion/world_bank_lpi.py
    |                                (manual by default; dags/comtrade_weekly_dag.py defines a weekly
    |                                 schedule, but only runs on one if you deploy Airflow yourself —
    |                                 see Orchestration below. No Airflow instance is deployed today.)
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
    gold_supplier_emissions                 — per-seller estimated Scope 3 kg CO2e + emissions intensity
    gold_supplier_risk_emissions_score      — reliability_score x emissions_intensity, percentile-blended
    gold_supplier_emission_swap_suggestions — high-emission suppliers paired with a lower-emission alternative
    ↓
FastAPI  (/suppliers — the CI-tested, verified path)
Streamlit dashboard  (charts direct from DuckDB, CI-tested; Trade-Partner Concentration
                       section reflects whatever the Comtrade pipeline was last run
                       against — manually, by default; see Orchestration below)
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
that means the *public* Cloud dashboard does not actually receive the Comtrade
pipeline's output at all (it shows a static synthetic example instead) — only a
self-hosted deployment sharing the same DuckDB file as the Comtrade pipeline does,
and only after that pipeline is actually run (manually, by default — see
Orchestration below for what it would take to put it on an actual schedule).

## Emissions Layer

A spend-based Scope 3 (purchased-goods) estimate for every supplier, built on EPA's
Supply Chain GHG Emission Factors (kg CO2e per 2022 USD, by NAICS-6 commodity): each
of Olist's 73 real product categories is mapped to a NAICS code
(`dbt/seeds/category_to_naics.csv`), order-item spend is converted from BRL to a
2022-USD-equivalent (FX + CPI vars in `dbt/dbt_project.yml`), and multiplied by that
category's emission factor. See `DECISIONS.md`, Phase 8, for the full methodology
and its stated limits — most importantly, **these are US emission factors applied to
Brazilian marketplace spend, a spend-based estimate (not activity-based), and the
category-to-NAICS mapping is judgment-based.** This is not a measured emissions
figure.

```bash
# Seeds (EPA factors + category mapping) load with everything else:
cd dbt && dbt build && cd ..

# Query the emissions-adjusted risk score directly:
python3 -c "
import duckdb
conn = duckdb.connect('data/duckdb/supply_chain.duckdb', read_only=True)
print(conn.execute('SELECT * FROM gold.gold_supplier_risk_emissions_score ORDER BY risk_emissions_score DESC LIMIT 10').fetchdf())
"
```

**The swap-suggestions table** (`gold_supplier_emission_swap_suggestions`) is the
actionable output: for each supplier in the top quartile of emissions intensity
within their own product category (var `emissions_swap_threshold_percentile`,
default 0.75), it finds the lowest-intensity same-category alternative that's no
worse on `reliability_score` (var `emissions_swap_risk_tolerance`, default 0 — the
"without raising late-delivery risk" constraint in this README's first line) and has
enough order volume to be a plausible switch (var `emissions_swap_min_order_volume`,
default 5). As of the last `dbt build`: 585 suppliers flagged with a qualifying
alternative, reductions in the 25-45% range on spot-checked rows. Visible in the
dashboard's **Emissions** tab, alongside a risk-vs-emissions scatter plot.

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
#     — the pipeline dags/comtrade_weekly_dag.py would run on a schedule if you deployed
#     Airflow (see Orchestration below); run manually here, safe to re-run (dedup on natural key)
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

`dags/comtrade_weekly_dag.py` defines a weekly schedule for the Comtrade
partner-breakdown pull + dbt refresh — but no Airflow instance is deployed running
it today; this is manual by default. Run it once without Airflow at all:

```bash
python -m ingestion.comtrade_pipeline                      # default: Brazil + Argentina, 3 products
python -m ingestion.comtrade_pipeline --reporters 76 --commodities 85 33
```

To actually run it on a schedule yourself, see [Docker](#docker) below —
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

Current test counts (`pytest tests/ --ignore=tests/test_agent.py` + `dbt build`,
excludes the live-LLM smoke test — see note above):

| Suite | Result |
|---|---|
| pytest | 29 passed |
| dbt (seeds + models + data tests) | PASS=81, WARN=0, ERROR=0 (3 seeds, 21 models, 57 data tests) |

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
