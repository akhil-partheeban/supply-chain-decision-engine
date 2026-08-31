"""Unit tests for the pipeline instrumentation report (scripts/pipeline_report.py)."""

import duckdb
import pytest

from scripts.pipeline_report import build_report, dbt_run_stats, schema_row_counts


@pytest.fixture()
def instrumented_db(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)

    conn.execute("CREATE SCHEMA bronze")
    conn.execute("CREATE SCHEMA silver")
    conn.execute("CREATE SCHEMA gold")

    conn.execute("CREATE TABLE bronze.orders AS SELECT * FROM (VALUES (1), (2), (3)) t(id)")
    conn.execute("CREATE TABLE silver.silver_orders AS SELECT * FROM (VALUES (1), (2)) t(id)")

    conn.execute("""
        CREATE TABLE gold.gold_supplier_scorecard AS
        SELECT * FROM (VALUES
            ('s1', 'HIGH'), ('s2', 'LOW'), ('s3', 'LOW'), ('s4', 'MEDIUM')
        ) t(seller_id, risk_tier)
    """)
    conn.execute("""
        CREATE TABLE gold.gold_concentration_risk AS
        SELECT * FROM (VALUES ('s1', 'HIGH'), ('s2', 'NORMAL')) t(seller_id, concentration_flag)
    """)
    conn.execute("""
        CREATE TABLE gold.gold_geo_concentration AS
        SELECT * FROM (VALUES ('SP', 'HIGH'), ('RJ', 'NORMAL')) t(seller_state, concentration_flag)
    """)
    conn.execute("""
        CREATE TABLE gold.gold_sourcing_cost_drivers AS
        SELECT * FROM (VALUES ('electronics', 'LOW')) t(product_category, freight_burden_tier)
    """)
    conn.execute("""
        CREATE TABLE gold.gold_executive_summary AS
        SELECT
            3   AS total_orders,
            4   AS total_suppliers,
            1000.0 AS total_revenue,
            0.08   AS overall_late_rate,
            75.0   AS avg_reliability_score,
            4.2    AS avg_review_score,
            500.0  AS hhi_index,
            'UNCONCENTRATED' AS hhi_interpretation,
            10.0   AS top5_supplier_revenue_share_pct,
            1      AS categories_scored
    """)
    conn.close()
    return db_path


def test_schema_row_counts(instrumented_db):
    conn = duckdb.connect(instrumented_db, read_only=True)
    counts = schema_row_counts(conn, "bronze")
    conn.close()
    assert counts == {"orders": 3}


def test_dbt_run_stats_missing_returns_none(tmp_path):
    assert dbt_run_stats(str(tmp_path / "nonexistent")) is None


def test_build_report_structure(instrumented_db, tmp_path):
    empty_raw_dir = tmp_path / "raw"
    empty_raw_dir.mkdir()

    report = build_report(
        db_path=instrumented_db,
        raw_dir=str(empty_raw_dir),
        dbt_target_dir=str(tmp_path / "no_dbt_target"),
    )

    assert report["layers"]["bronze"]["total_rows"] == 3
    assert report["layers"]["silver"]["total_rows"] == 2
    assert report["dbt_run"] is None

    biz = report["business_metrics"]
    assert biz["total_suppliers_scored"] == 4

    # 1 of 4 scorecard rows is HIGH risk
    high_risk = biz["flagging_rates"]["high_risk_suppliers"]
    assert high_risk == {"total": 4, "flagged": 1, "flag_rate_pct": 25.0}

    # 1 of 2 concentration_risk rows is HIGH
    single_source = biz["flagging_rates"]["single_supplier_dependency"]
    assert single_source == {"total": 2, "flagged": 1, "flag_rate_pct": 50.0}
