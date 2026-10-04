"""Tests for api/main.py's /health endpoint — specifically that the top-level
"status" field actually reflects the DB check result, not a hardcoded "ok".
"""

import duckdb
from fastapi.testclient import TestClient

from api.main import app


def test_health_ok(tmp_path, monkeypatch):
    db_path = str(tmp_path / "healthy.duckdb")
    duckdb.connect(db_path).close()
    monkeypatch.setenv("DUCKDB_PATH", db_path)

    client = TestClient(app)
    resp = client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["db"] == "ok"


def test_health_reports_db_failure(tmp_path, monkeypatch):
    missing_db_path = str(tmp_path / "does_not_exist.duckdb")
    monkeypatch.setenv("DUCKDB_PATH", missing_db_path)

    client = TestClient(app)
    resp = client.get("/health")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] != "ok"
    assert "does_not_exist.duckdb" in body["db"]
