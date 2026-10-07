"""Tests for api/routers/decisions.py's per-IP rate limit.

Mocks agent.decision_agent.run_decision_agent so these tests never call the real
Anthropic API — this is a test of the rate-limit logic itself, not the agent (see
tests/test_agent.py for the live, uncovered-in-CI agent smoke test).
"""

from fastapi.testclient import TestClient

import api.routers.decisions as decisions_module
from api.main import app


async def _fake_run_decision_agent(question, context=None):
    return {"answer": "fake answer", "sql_used": [], "action_items": ["fake action"]}


def test_requests_within_limit_succeed(monkeypatch):
    monkeypatch.setattr(decisions_module, "run_decision_agent", _fake_run_decision_agent)
    decisions_module._request_log.clear()
    client = TestClient(app)
    headers = {"x-forwarded-for": "10.0.0.1"}

    for _ in range(decisions_module.RATE_LIMIT_MAX_REQUESTS):
        resp = client.post("/decisions/ask", json={"question": "q"}, headers=headers)
        assert resp.status_code == 200
        assert resp.json()["answer"] == "fake answer"


def test_request_over_limit_is_rejected(monkeypatch):
    monkeypatch.setattr(decisions_module, "run_decision_agent", _fake_run_decision_agent)
    decisions_module._request_log.clear()
    client = TestClient(app)
    headers = {"x-forwarded-for": "10.0.0.2"}

    for _ in range(decisions_module.RATE_LIMIT_MAX_REQUESTS):
        client.post("/decisions/ask", json={"question": "q"}, headers=headers)

    resp = client.post("/decisions/ask", json={"question": "one too many"}, headers=headers)
    assert resp.status_code == 429


def test_different_ips_have_independent_limits(monkeypatch):
    monkeypatch.setattr(decisions_module, "run_decision_agent", _fake_run_decision_agent)
    decisions_module._request_log.clear()
    client = TestClient(app)

    for _ in range(decisions_module.RATE_LIMIT_MAX_REQUESTS):
        client.post(
            "/decisions/ask", json={"question": "q"}, headers={"x-forwarded-for": "10.0.0.3"}
        )

    # A different IP should not be affected by 10.0.0.3's exhausted limit.
    resp = client.post(
        "/decisions/ask", json={"question": "q"}, headers={"x-forwarded-for": "10.0.0.4"}
    )
    assert resp.status_code == 200


def test_client_ip_prefers_x_forwarded_for():
    from unittest.mock import MagicMock

    request = MagicMock()
    request.headers.get.return_value = "203.0.113.5, 10.0.0.1"
    assert decisions_module._client_ip(request) == "203.0.113.5"
