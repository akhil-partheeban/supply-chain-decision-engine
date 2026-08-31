"""Tests for ingestion/comtrade_pipeline.py — the weekly-pull orchestration layer."""

import duckdb
import responses

from ingestion.comtrade_pipeline import detect_latest_period, run_weekly_pull


def _preview_url():
    return "https://comtradeapi.un.org/public/v1/preview/C/A/HS"


@responses.activate
def test_detect_latest_period_finds_first_year_with_data(monkeypatch):
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)
    monkeypatch.setattr("ingestion.comtrade_pipeline.time.sleep", lambda _: None)

    # Newest candidate year empty, next one has data — should stop there.
    responses.add(responses.GET, _preview_url(), json={"data": []}, status=200)
    responses.add(responses.GET, _preview_url(), json={"data": [{"fobvalue": 1.0}]}, status=200)

    period = detect_latest_period("76", candidate_years=["2026", "2025"])

    assert period == "2025"
    assert len(responses.calls) == 2


@responses.activate
def test_detect_latest_period_raises_if_nothing_found(monkeypatch):
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)
    monkeypatch.setattr("ingestion.comtrade_pipeline.time.sleep", lambda _: None)

    responses.add(responses.GET, _preview_url(), json={"data": []}, status=200)

    try:
        detect_latest_period("76", candidate_years=["2026"])
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "No available Comtrade period" in str(exc)


@responses.activate
def test_run_weekly_pull_fetches_all_reporter_commodity_combos_and_upserts(tmp_path, monkeypatch):
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)
    monkeypatch.setattr("ingestion.comtrade_pipeline.time.sleep", lambda _: None)

    # detect_latest_period probe (reporter 76, TOTAL)
    responses.add(responses.GET, _preview_url(), json={"data": [{"probe": True}]}, status=200)

    def _row(reporter, commodity, partner):
        return {
            "typeCode": "C", "freqCode": "A", "period": "2025", "reporterCode": reporter,
            "flowCode": "M", "partnerCode": partner, "partner2Code": 0,
            "cmdCode": commodity, "customsCode": "C00", "motCode": "0", "fobvalue": 100.0,
        }

    # 2 reporters x 2 commodities = 4 calls, one row each
    for reporter in ("76", "32"):
        for commodity in ("85", "33"):
            responses.add(
                responses.GET, _preview_url(),
                json={"data": [_row(reporter, commodity, 156)]}, status=200,
            )

    db_path = str(tmp_path / "test.duckdb")
    summary = run_weekly_pull(db_path, reporters=["76", "32"], commodities=["85", "33"], run_id="test-run")

    assert summary["period"] == "2025"
    assert summary["fetched"] == 4
    assert summary["rows_after"] == 4
    assert summary["truncated"] == []
    assert len(responses.calls) == 1 + 4  # 1 probe + 4 pull calls

    conn = duckdb.connect(db_path, read_only=True)
    n = conn.execute("SELECT count(*) FROM bronze.comtrade_trade_flows").fetchone()[0]
    assert n == 4
    conn.close()


@responses.activate
def test_run_weekly_pull_flags_truncation(tmp_path, monkeypatch):
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)
    monkeypatch.setattr("ingestion.comtrade_pipeline.time.sleep", lambda _: None)
    monkeypatch.setattr("ingestion.comtrade_pipeline.PREVIEW_ROW_CAP", 2)  # small cap, easy to trip in a test

    responses.add(responses.GET, _preview_url(), json={"data": [{"probe": True}]}, status=200)  # period probe

    truncated_rows = [
        {
            "typeCode": "C", "freqCode": "A", "period": "2025", "reporterCode": "76",
            "flowCode": "M", "partnerCode": p, "partner2Code": 0,
            "cmdCode": "85", "customsCode": "C00", "motCode": "0", "fobvalue": 1.0,
        }
        for p in (156, 842)  # exactly 2 rows == the patched cap
    ]
    responses.add(responses.GET, _preview_url(), json={"data": truncated_rows}, status=200)

    db_path = str(tmp_path / "test.duckdb")
    summary = run_weekly_pull(db_path, reporters=["76"], commodities=["85"], run_id="test-run")

    assert len(summary["truncated"]) == 1
    assert summary["truncated"][0]["reporter"] == "76"
    assert summary["truncated"][0]["rows"] == 2


@responses.activate
def test_run_weekly_pull_is_idempotent_across_runs(tmp_path, monkeypatch):
    """Two 'weekly' runs pulling the same period should not double the row count —
    this is the actual dedup guarantee the task asked for, exercised at the
    pipeline level (not just upsert_bronze in isolation)."""
    monkeypatch.delenv("COMTRADE_API_KEY", raising=False)
    monkeypatch.setattr("ingestion.comtrade_pipeline.time.sleep", lambda _: None)

    def _row(fobvalue):
        return {
            "typeCode": "C", "freqCode": "A", "period": "2025", "reporterCode": "76",
            "flowCode": "M", "partnerCode": 156, "partner2Code": 0,
            "cmdCode": "85", "customsCode": "C00", "motCode": "0", "fobvalue": fobvalue,
        }

    # Run 1: probe + 1 pull call
    responses.add(responses.GET, _preview_url(), json={"data": [{"probe": True}]}, status=200)
    responses.add(responses.GET, _preview_url(), json={"data": [_row(100.0)]}, status=200)
    # Run 2 (same period, revised value): probe + 1 pull call
    responses.add(responses.GET, _preview_url(), json={"data": [{"probe": True}]}, status=200)
    responses.add(responses.GET, _preview_url(), json={"data": [_row(999.0)]}, status=200)

    db_path = str(tmp_path / "test.duckdb")
    run_weekly_pull(db_path, reporters=["76"], commodities=["85"], run_id="run-1")
    summary2 = run_weekly_pull(db_path, reporters=["76"], commodities=["85"], run_id="run-2")

    assert summary2["rows_after"] == 1  # still one row, not two
    conn = duckdb.connect(db_path, read_only=True)
    fobvalue = conn.execute("SELECT fobvalue FROM bronze.comtrade_trade_flows").fetchone()[0]
    assert fobvalue == 999.0  # revised value won
    conn.close()
