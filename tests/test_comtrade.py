"""
Tests for ingestion/comtrade.py.

The core thing under test is the URL structure: typeCode/freqCode/clCode must be
PATH segments (/preview/C/A/HS or /get/C/A/HS), not query parameters — an earlier
version of this module put them in the query string, which 404'd on every real call
and was never caught because nothing ever ran this module end-to-end. These tests
pin the correct URL shape with `responses` so that bug can't silently come back.
"""

import duckdb
import responses

from ingestion.comtrade import fetch_trade_flows, load_bronze, upsert_bronze


@responses.activate
def test_fetch_trade_flows_uses_path_based_preview_url(monkeypatch):
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)  # force the free preview path

    responses.add(
        responses.GET,
        "https://comtradeapi.un.org/public/v1/preview/C/A/HS",
        json={"data": [{"reporterCode": 76, "flowCode": "M", "fobvalue": 100.0}]},
        status=200,
    )

    records = fetch_trade_flows(reporter="76", period="2023", trade_flow="M")

    assert records == [{"reporterCode": 76, "flowCode": "M", "fobvalue": 100.0}]
    # Confirm typeCode/freqCode/clCode landed in the path, not the query string.
    request_url = responses.calls[0].request.url
    assert "/preview/C/A/HS" in request_url
    assert "typeCode=" not in request_url
    assert "clCode=" not in request_url


@responses.activate
def test_fetch_trade_flows_uses_data_api_with_real_key(monkeypatch):
    monkeypatch.setenv("COMTRADE_API_KEY", "a_real_looking_key_123456")

    responses.add(
        responses.GET,
        "https://comtradeapi.un.org/data/v1/get/C/A/HS",
        json={"data": [{"reporterCode": 76}]},
        status=200,
    )

    records = fetch_trade_flows(reporter="76")
    assert records == [{"reporterCode": 76}]
    assert "subscription-key=a_real_looking_key_123456" in responses.calls[0].request.url


@responses.activate
def test_fetch_trade_flows_retries_on_429(monkeypatch):
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)

    responses.add(responses.GET, "https://comtradeapi.un.org/public/v1/preview/C/A/HS", status=429)
    responses.add(
        responses.GET,
        "https://comtradeapi.un.org/public/v1/preview/C/A/HS",
        json={"data": [{"ok": True}]},
        status=200,
    )

    records = fetch_trade_flows()

    assert records == [{"ok": True}]
    assert len(responses.calls) == 2


def test_load_bronze_creates_table(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA bronze")

    row_count = load_bronze(conn, [{"reporterCode": 76, "fobvalue": 100.0}])

    assert row_count == 1
    result = conn.execute("SELECT reporterCode, fobvalue FROM bronze.comtrade_trade_flows").fetchone()
    assert result == (76, 100.0)
    conn.close()


def test_load_bronze_empty_records_is_a_noop(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA bronze")

    assert load_bronze(conn, []) == 0
    conn.close()


def _record(partner_code, cmd_code="85", fobvalue=100.0, period="2023", reporter=76, flow="M"):
    return {
        "typeCode": "C", "freqCode": "A", "period": period, "reporterCode": reporter,
        "flowCode": flow, "partnerCode": partner_code, "partner2Code": 0,
        "cmdCode": cmd_code, "customsCode": "C00", "motCode": "0", "fobvalue": fobvalue,
    }


def test_upsert_bronze_creates_table_on_first_run(tmp_path):
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA bronze")

    result = upsert_bronze(conn, [_record(156)], run_id="run-1")

    assert result == {"fetched": 1, "rows_before": 0, "rows_after": 1}
    conn.close()


def test_upsert_bronze_replaces_revised_rows_and_adds_new_ones(tmp_path):
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA bronze")

    upsert_bronze(conn, [_record(156, fobvalue=1000.0)], run_id="run-1")
    result = upsert_bronze(
        conn,
        [_record(156, fobvalue=1234.0), _record(842, fobvalue=500.0)],  # 156 revised, 842 new
        run_id="run-2",
    )

    assert result == {"fetched": 2, "rows_before": 1, "rows_after": 2}
    rows = conn.execute(
        "SELECT partnerCode, fobvalue FROM bronze.comtrade_trade_flows ORDER BY partnerCode"
    ).fetchall()
    assert rows == [(156, 1234.0), (842, 500.0)]  # revised value kept, not duplicated
    conn.close()


def test_upsert_bronze_is_idempotent_on_retry(tmp_path):
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA bronze")

    records = [_record(156), _record(842)]
    upsert_bronze(conn, records, run_id="run-1")
    result = upsert_bronze(conn, records, run_id="run-1-retry")  # simulates an Airflow task retry

    assert result == {"fetched": 2, "rows_before": 2, "rows_after": 2}
    n = conn.execute("SELECT count(*) FROM bronze.comtrade_trade_flows").fetchone()[0]
    assert n == 2
    conn.close()


def test_upsert_bronze_distinguishes_different_periods_and_products(tmp_path):
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA bronze")

    upsert_bronze(conn, [_record(156, cmd_code="85", period="2023")], run_id="run-1")
    result = upsert_bronze(conn, [_record(156, cmd_code="33", period="2023")], run_id="run-2")  # different product
    result2 = upsert_bronze(conn, [_record(156, cmd_code="85", period="2024")], run_id="run-3")  # different period

    assert result["rows_after"] == 2
    assert result2["rows_after"] == 3  # nothing got overwritten — all three keys are genuinely distinct
    conn.close()
