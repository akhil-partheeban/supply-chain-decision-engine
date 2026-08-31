"""
Pipeline instrumentation report: the honest, real numbers behind this project.

Prints a snapshot of the full bronze -> silver -> gold pipeline — row counts per
layer, data volume, dbt run timing (parsed from dbt's own run_results.json — no
re-implementation of what dbt already measures), and the gold-layer business
metrics that make this a decision engine rather than a reporting tool: how many
suppliers were scored, and what share of suppliers/states/categories actually
tripped a risk flag.

These are the numbers this project's DECISIONS.md and README cite — this script
is how they were produced, and it's meant to be re-run (not hand-copied) whenever
the underlying data or models change, so those citations never go stale silently.

Usage:
    python -m scripts.pipeline_report
    python -m scripts.pipeline_report --db path/to/custom.duckdb
    python -m scripts.pipeline_report --json reports/pipeline_metrics.json
    python -m scripts.pipeline_report --no-json
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")
DEFAULT_RAW_DIR = "data/raw"
DEFAULT_DBT_TARGET_DIR = "dbt/target"
DEFAULT_JSON_PATH = "reports/pipeline_metrics.json"


# ── Layer-level row counts ──────────────────────────────────────────────────────

def schema_row_counts(conn: duckdb.DuckDBPyConnection, schema: str) -> dict[str, int]:
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY 1",
        [schema],
    ).fetchall()
    return {
        t[0]: conn.execute(f'SELECT count(*) FROM "{schema}"."{t[0]}"').fetchone()[0]
        for t in tables
    }


# ── Data volume ──────────────────────────────────────────────────────────────────

def raw_input_volume_mb(raw_dir: Path) -> float | None:
    if not raw_dir.exists():
        return None
    total = sum(f.stat().st_size for f in raw_dir.glob("*.csv"))
    return round(total / (1024 * 1024), 2)


def duckdb_file_size_mb(db_path: str) -> float | None:
    p = Path(db_path)
    if not p.exists():
        return None
    return round(p.stat().st_size / (1024 * 1024), 2)


# ── dbt run timing (parsed from dbt's own run_results.json artifact) ────────────

def dbt_run_stats(target_dir: str) -> dict | None:
    run_results_path = Path(target_dir) / "run_results.json"
    if not run_results_path.exists():
        return None

    data = json.loads(run_results_path.read_text())
    model_results = [
        r for r in data.get("results", []) if r["unique_id"].startswith("model.")
    ]
    test_results = [
        r for r in data.get("results", []) if r["unique_id"].startswith("test.")
    ]

    return {
        "generated_at": data.get("metadata", {}).get("generated_at"),
        "total_elapsed_seconds": round(data.get("elapsed_time", 0), 3),
        "models_run": len(model_results),
        "models_failed": sum(1 for r in model_results if r["status"] != "success"),
        "tests_run": len(test_results),
        "tests_failed": sum(1 for r in test_results if r["status"] != "pass"),
        "slowest_models": sorted(
            (
                {
                    "model": r["unique_id"].split(".")[-1],
                    "execution_time_seconds": round(r["execution_time"], 4),
                }
                for r in model_results
            ),
            key=lambda x: x["execution_time_seconds"],
            reverse=True,
        )[:5],
    }


# ── Gold-layer business + flagging metrics ──────────────────────────────────────

def gold_business_metrics(conn: duckdb.DuckDBPyConnection) -> dict | None:
    tables = {t[0] for t in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'"
    ).fetchall()}
    required = {
        "gold_executive_summary", "gold_supplier_scorecard",
        "gold_concentration_risk", "gold_geo_concentration", "gold_sourcing_cost_drivers",
    }
    if not required.issubset(tables):
        return None

    exec_summary = conn.execute("SELECT * FROM gold.gold_executive_summary").fetchdf().iloc[0].to_dict()

    def flag_rate(table: str, flag_col: str, flag_value: str) -> dict:
        row = conn.execute(f"""
            SELECT
                count(*) AS total,
                sum(CASE WHEN {flag_col} = '{flag_value}' THEN 1 ELSE 0 END) AS flagged
            FROM gold.{table}
        """).fetchone()
        total, flagged = row
        return {
            "total": total,
            "flagged": flagged,
            "flag_rate_pct": round(flagged / total * 100, 2) if total else 0.0,
        }

    return {
        "total_suppliers_scored": int(exec_summary["total_suppliers"]),
        "total_orders": int(exec_summary["total_orders"]),
        "total_revenue": round(float(exec_summary["total_revenue"]), 2),
        "overall_late_delivery_rate_pct": round(float(exec_summary["overall_late_rate"]) * 100, 2),
        "avg_reliability_score": float(exec_summary["avg_reliability_score"]),
        "avg_review_score": float(exec_summary["avg_review_score"]),
        "supplier_hhi": float(exec_summary["hhi_index"]),
        "supplier_hhi_interpretation": exec_summary["hhi_interpretation"],
        "top5_supplier_revenue_share_pct": float(exec_summary["top5_supplier_revenue_share_pct"]),
        "categories_scored": int(exec_summary["categories_scored"]),
        "flagging_rates": {
            "high_risk_suppliers":       flag_rate("gold_supplier_scorecard", "risk_tier", "HIGH"),
            "single_supplier_dependency": flag_rate("gold_concentration_risk", "concentration_flag", "HIGH"),
            "geographic_concentration":  flag_rate("gold_geo_concentration", "concentration_flag", "HIGH"),
            "high_freight_burden_categories": flag_rate("gold_sourcing_cost_drivers", "freight_burden_tier", "HIGH"),
        },
    }


# ── Report assembly ───────────────────────────────────────────────────────────────

def build_report(db_path: str, raw_dir: str, dbt_target_dir: str) -> dict:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        bronze = schema_row_counts(conn, "bronze")
        silver = schema_row_counts(conn, "silver")
        gold = schema_row_counts(conn, "gold")
        business = gold_business_metrics(conn)
    finally:
        conn.close()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": db_path,
        "data_volume": {
            "raw_input_csv_mb": raw_input_volume_mb(Path(raw_dir)),
            "duckdb_file_mb": duckdb_file_size_mb(db_path),
        },
        "layers": {
            "bronze": {"tables": len(bronze), "total_rows": sum(bronze.values()), "row_counts": bronze},
            "silver": {"tables": len(silver), "total_rows": sum(silver.values()), "row_counts": silver},
            "gold":   {"tables": len(gold),   "total_rows": sum(gold.values()),   "row_counts": gold},
        },
        "dbt_run": dbt_run_stats(dbt_target_dir),
        "business_metrics": business,
    }


# ── Console rendering ──────────────────────────────────────────────────────────────

def print_report(report: dict) -> None:
    print("=" * 72)
    print("PIPELINE INSTRUMENTATION REPORT")
    print(f"Generated: {report['generated_at']}")
    print(f"Database:  {report['db_path']}")
    print("=" * 72)

    vol = report["data_volume"]
    print("\nDATA VOLUME")
    if vol["raw_input_csv_mb"] is not None:
        print(f"  Raw input CSVs:     {vol['raw_input_csv_mb']:>10.1f} MB")
    if vol["duckdb_file_mb"] is not None:
        print(f"  DuckDB file:        {vol['duckdb_file_mb']:>10.1f} MB")

    print("\nLAYER ROW COUNTS")
    for layer_name in ("bronze", "silver", "gold"):
        layer = report["layers"][layer_name]
        print(f"  {layer_name.upper():<8} {layer['tables']:>2} tables   {layer['total_rows']:>10,} rows")
        for table, count in layer["row_counts"].items():
            print(f"      {table:<40} {count:>10,}")

    dbt_run = report["dbt_run"]
    if dbt_run:
        print("\nDBT RUN (most recent `dbt build`/`dbt run`)")
        print(f"  Models run:    {dbt_run['models_run']} ({dbt_run['models_failed']} failed)")
        print(f"  Tests run:     {dbt_run['tests_run']} ({dbt_run['tests_failed']} failed)")
        print(f"  Total elapsed: {dbt_run['total_elapsed_seconds']}s")
        print("  Slowest models:")
        for m in dbt_run["slowest_models"]:
            print(f"      {m['model']:<35} {m['execution_time_seconds']:.4f}s")
    else:
        print("\nDBT RUN")
        print("  No dbt/target/run_results.json found — run `dbt build` first.")

    biz = report["business_metrics"]
    if biz:
        print("\nGOLD-LAYER BUSINESS METRICS")
        print(f"  Suppliers scored:            {biz['total_suppliers_scored']:,}")
        print(f"  Orders analyzed:             {biz['total_orders']:,}")
        print(f"  Total revenue:               ${biz['total_revenue']:,.2f}")
        print(f"  Overall late-delivery rate:  {biz['overall_late_delivery_rate_pct']}%")
        print(f"  Avg reliability score:       {biz['avg_reliability_score']} / 100")
        print(f"  Avg review score:            {biz['avg_review_score']} / 5")
        print(f"  Supplier concentration (HHI): {biz['supplier_hhi']} ({biz['supplier_hhi_interpretation']})")
        print(f"  Top-5 supplier revenue share: {biz['top5_supplier_revenue_share_pct']}%")
        print(f"  Product categories scored:    {biz['categories_scored']}")
        print("\n  FLAGGING / DETECTION RATES")
        for name, r in biz["flagging_rates"].items():
            label = name.replace("_", " ")
            print(f"      {label:<32} {r['flagged']:>5,} / {r['total']:>5,}  ({r['flag_rate_pct']}%)")
    else:
        print("\nGOLD-LAYER BUSINESS METRICS")
        print("  Gold tables not found — run `dbt build` first.")

    print("\n" + "=" * 72)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Print/log pipeline instrumentation metrics")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to DuckDB file")
    parser.add_argument("--raw-dir", default=DEFAULT_RAW_DIR, help="Directory containing raw CSVs")
    parser.add_argument("--dbt-target-dir", default=DEFAULT_DBT_TARGET_DIR, help="dbt target/ directory")
    parser.add_argument("--json", default=DEFAULT_JSON_PATH, help="Path to write the JSON report")
    parser.add_argument("--no-json", action="store_true", help="Skip writing the JSON report file")
    args = parser.parse_args()

    if not Path(args.db).exists():
        raise SystemExit(f"DuckDB file not found at {args.db} — run ingestion + dbt build first.")

    report = build_report(args.db, args.raw_dir, args.dbt_target_dir)
    print_report(report)

    if not args.no_json:
        out_path = Path(args.json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, default=str))
        print(f"\nJSON report written to {out_path}")


if __name__ == "__main__":
    main()
