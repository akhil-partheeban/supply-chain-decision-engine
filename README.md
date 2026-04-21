# Supply Chain Decision Engine

An end-to-end analytics and AI decision platform for global supply chain analysis, built on a medallion lakehouse architecture using DuckDB.

## Data Sources

| Source | Description |
|--------|-------------|
| [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Orders, sellers, products, reviews, payments |
| [UN Comtrade API](https://comtradeapi.un.org/) | Global trade flow data |
| [World Bank LPI](https://lpi.worldbank.org/) | Logistics Performance Index by country |

## Architecture

```
data/raw/ (CSVs)
    ↓  ingestion/load_bronze.py
DuckDB bronze schema  (raw tables + _source_file, _loaded_at metadata)
    ↓  dbt silver models
DuckDB silver schema  (cleaned, typed, enriched)
    ↓  dbt gold models
DuckDB gold schema    (aggregated scorecards, KPIs)
    ↓
FastAPI  (/suppliers, /decisions/ask)
LangChain agent  (SQL tool + OpenAI)
Streamlit dashboard
```

## Quickstart

```bash
# 1. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# edit .env — set OPENAI_API_KEY, COMTRADE_API_KEY, etc.

# 3. Download Olist CSVs from Kaggle and place them in data/raw/

# 4. Load bronze layer
python -m ingestion.load_bronze

# 5. Run dbt silver + gold transformations
cd dbt && dbt run

# 6. Start API
uvicorn api.main:app --reload

# 7. Start dashboard (separate terminal)
streamlit run dashboard/app.py
```

## Docker

```bash
cd docker
docker compose up --build
```

API: http://localhost:8000  
Dashboard: http://localhost:8501  
API docs: http://localhost:8000/docs

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
```
