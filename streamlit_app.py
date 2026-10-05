"""
Streamlit Cloud entry point for the Supply Chain Decision Engine.

Three possible data sources, tried in this order:
  1. A real local DB already exists at DUCKDB_PATH (e.g. local dev, Docker) —
     used as-is, untouched by anything below. This is the real-DB path and
     nothing in this file should change its behavior.
  2. No real DB, but data/demo/gold_snapshot.duckdb is committed to the repo
     (all 14 gold tables, plus silver.silver_comtrade_partner_flows — the one
     silver table the dashboard queries directly, for the Trade-Partner
     Concentration panel's freshness caption; no bronze, no raw Olist rows) —
     used directly. This is what Streamlit Community Cloud serves: real
     pipeline output, not synthetic data, with no AWS credentials or network
     fetch required (the snapshot is a committed file). See README.md and
     DECISIONS.md for why this snapshot shape, and the Olist dataset's
     CC BY-NC-SA 4.0 attribution.
  3. Neither exists — the sample data generator builds a synthetic database
     so the dashboard still works. dashboard/app.py shows a demo-mode banner
     whenever this path is taken, via the DEMO_MODE env var set below.
"""

import os
import sys
from pathlib import Path

# ── Ensure project root is importable ─────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# ── Resolve DB path and propagate as an absolute path ─────────────────────────
_default_db = ROOT / "data" / "duckdb" / "supply_chain.duckdb"
_gold_snapshot = ROOT / "data" / "demo" / "gold_snapshot.duckdb"
DB_PATH = os.getenv("DUCKDB_PATH", str(_default_db))

if not Path(DB_PATH).exists():
    if _gold_snapshot.exists():
        DB_PATH = str(_gold_snapshot)
    else:
        from data.sample_data import build_sample_db  # noqa: E402
        build_sample_db(DB_PATH)
        os.environ["DEMO_MODE"] = "1"

os.environ["DUCKDB_PATH"] = DB_PATH  # dashboard reads this env var

# ── Hand off to the dashboard (runs in this module's global scope) ─────────────
_dashboard = ROOT / "dashboard" / "app.py"
exec(open(_dashboard).read())  # noqa: S102
