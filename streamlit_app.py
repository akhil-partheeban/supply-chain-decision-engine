"""
Streamlit Cloud entry point for the Supply Chain Decision Engine.

On first run (no DuckDB file present) the sample data generator builds a
synthetic database automatically so the dashboard works without the real
Kaggle CSVs.  Subsequent runs skip the build step and go straight to the
dashboard.
"""

import os
import sys
from pathlib import Path

# ── Ensure project root is importable ─────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# ── Resolve DB path and propagate as an absolute path ─────────────────────────
_default_db = ROOT / "data" / "duckdb" / "supply_chain.duckdb"
DB_PATH = os.getenv("DUCKDB_PATH", str(_default_db))
os.environ["DUCKDB_PATH"] = DB_PATH  # dashboard reads this env var

# ── Bootstrap sample data if the database doesn't exist ───────────────────────
if not Path(DB_PATH).exists():
    from data.sample_data import build_sample_db  # noqa: E402
    build_sample_db(DB_PATH)

# ── Hand off to the dashboard (runs in this module's global scope) ─────────────
_dashboard = ROOT / "dashboard" / "app.py"
exec(open(_dashboard).read())  # noqa: S102
