"""Tests for agent/decision_agent.py's cost/abuse guards (MAX_TURNS, timeouts) —
against a mocked Anthropic client, never the real API. This is the kind of
coverage DECISIONS.md (Phase 7, "What this CI gate does not cover") named as the
honest next step for this file and never built until now: real regression
protection for the tool-calling loop's structure without touching a live key.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from agent import decision_agent


def _tool_use_response():
    """A response that always asks to call run_sql again, forcing the loop to
    keep going — used to prove MAX_TURNS actually bounds it."""
    block = SimpleNamespace(
        type="tool_use", id="toolu_1", name="run_sql", input={"query": "SELECT 1"}
    )
    return SimpleNamespace(content=[block], stop_reason="tool_use")


def test_ask_stops_at_max_turns(monkeypatch):
    fake_client = MagicMock()
    fake_client.messages.create.return_value = _tool_use_response()
    monkeypatch.setattr(decision_agent.anthropic, "Anthropic", lambda **kwargs: fake_client)
    monkeypatch.setattr(decision_agent, "_dispatch", lambda name, inp: "fake tool result")

    result = decision_agent.ask("a question that never resolves")

    assert fake_client.messages.create.call_count == decision_agent.MAX_TURNS
    assert str(decision_agent.MAX_TURNS) in result["answer"]
    assert result["action_items"] == ["Narrow the question and try again."]


def test_ask_does_not_call_extract_action_items_fallback_on_turn_limit(monkeypatch):
    """Hitting the turn limit must not trigger _extract_action_items' own Claude
    API call fallback — that would burn an extra call on exactly the request
    MAX_TURNS exists to cap."""
    fake_client = MagicMock()
    fake_client.messages.create.return_value = _tool_use_response()
    monkeypatch.setattr(decision_agent.anthropic, "Anthropic", lambda **kwargs: fake_client)
    monkeypatch.setattr(decision_agent, "_dispatch", lambda name, inp: "fake tool result")

    def _fail_if_called(answer):
        raise AssertionError("_extract_action_items should not run on turn-limit path")

    monkeypatch.setattr(decision_agent, "_extract_action_items", _fail_if_called)

    decision_agent.ask("a question that never resolves")  # must not raise


def test_run_decision_agent_offloads_to_thread_and_respects_timeout(monkeypatch):
    def _slow_ask(question):
        import time
        time.sleep(0.2)
        return {"answer": "too slow", "sql_used": [], "action_items": []}

    monkeypatch.setattr(decision_agent, "ask", _slow_ask)
    monkeypatch.setattr(decision_agent, "REQUEST_TIMEOUT_SECONDS", 0.05)

    result = asyncio.run(decision_agent.run_decision_agent("question"))

    assert "timeout" in result["answer"].lower()
    assert result["sql_used"] == []


def test_run_decision_agent_returns_real_result_within_timeout(monkeypatch):
    monkeypatch.setattr(
        decision_agent,
        "ask",
        lambda question: {"answer": "real answer", "sql_used": ["SELECT 1"], "action_items": ["a"]},
    )

    result = asyncio.run(decision_agent.run_decision_agent("question"))

    assert result["answer"] == "real answer"
    assert result["sql_used"] == ["SELECT 1"]
