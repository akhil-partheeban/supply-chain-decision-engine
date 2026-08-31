"""Tests for ingestion/world_bank_lpi.py."""

import duckdb
import responses

from ingestion.world_bank_lpi import LPI_INDICATORS, fetch_indicator, load_bronze


def _wb_payload(page, pages, rows):
    return [{"page": page, "pages": pages}, rows]


def _row(country_code="BRA", country_name="Brazil", indicator="LP.LPI.OVRL.XQ", year="2022", value=3.2):
    return {
        "countryiso3code": country_code,
        "country": {"value": country_name},
        "indicator": {"id": indicator, "value": "Logistics performance index"},
        "date": year,
        "value": value,
    }


@responses.activate
def test_fetch_indicator_single_page():
    responses.add(
        responses.GET,
        "https://api.worldbank.org/v2/country/BRA/indicator/LP.LPI.OVRL.XQ",
        json=_wb_payload(1, 1, [_row()]),
        status=200,
    )

    records = fetch_indicator("LP.LPI.OVRL.XQ", country="BRA")

    assert records == [{
        "country_code": "BRA",
        "country_name": "Brazil",
        "indicator_code": "LP.LPI.OVRL.XQ",
        "indicator_name": "Logistics performance index",
        "year": "2022",
        "value": 3.2,
    }]


@responses.activate
def test_fetch_indicator_paginates():
    url = "https://api.worldbank.org/v2/country/all/indicator/LP.LPI.OVRL.XQ"
    responses.add(responses.GET, url, json=_wb_payload(1, 2, [_row(year="2022")]), status=200)
    responses.add(responses.GET, url, json=_wb_payload(2, 2, [_row(year="2020")]), status=200)

    records = fetch_indicator("LP.LPI.OVRL.XQ", country="all")

    assert len(records) == 2
    assert {r["year"] for r in records} == {"2022", "2020"}
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_indicator_retries_on_timeout(monkeypatch):
    import requests

    url = "https://api.worldbank.org/v2/country/BRA/indicator/LP.LPI.OVRL.XQ"
    responses.add(responses.GET, url, body=requests.exceptions.ReadTimeout("slow"))
    responses.add(responses.GET, url, json=_wb_payload(1, 1, [_row()]), status=200)

    monkeypatch.setattr("ingestion.world_bank_lpi.time.sleep", lambda _: None)  # skip real backoff delay

    records = fetch_indicator("LP.LPI.OVRL.XQ", country="BRA")

    assert len(records) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_load_bronze_fetches_every_indicator(tmp_path):
    for indicator in LPI_INDICATORS:
        responses.add(
            responses.GET,
            f"https://api.worldbank.org/v2/country/BRA/indicator/{indicator}",
            json=_wb_payload(1, 1, [_row(indicator=indicator)]),
            status=200,
        )

    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA bronze")

    row_count = load_bronze(conn, country="BRA")

    assert row_count == len(LPI_INDICATORS)
    n_in_db = conn.execute("SELECT count(*) FROM bronze.world_bank_lpi").fetchone()[0]
    assert n_in_db == len(LPI_INDICATORS)
    conn.close()
