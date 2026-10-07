"""AI-assisted supply chain decision endpoints."""

import time

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from agent.decision_agent import run_decision_agent

router = APIRouter()

# Simple in-memory per-IP sliding-window rate limit. No external dependency (no
# Redis, no slowapi) — this is a single App Runner instance with no connection
# pool anywhere else in this project (see api/routers/suppliers.py), so in-memory
# state matching that same assumption is consistent, not a shortcut. Resets on
# every container restart/redeploy, which is fine for its purpose: bounding
# Anthropic API spend from one abusive caller, not precise global accounting.
RATE_LIMIT_MAX_REQUESTS = 5
RATE_LIMIT_WINDOW_SECONDS = 60
_request_log: dict[str, list[float]] = {}


def _client_ip(request: Request) -> str:
    # App Runner sits behind a managed load balancer — request.client.host alone
    # would be the LB's address, not the real caller's, making every caller share
    # one rate-limit bucket. X-Forwarded-For's first entry is the original client.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(ip: str) -> None:
    now = time.monotonic()
    window_start = now - RATE_LIMIT_WINDOW_SECONDS
    recent = [t for t in _request_log.get(ip, []) if t > window_start]
    if len(recent) >= RATE_LIMIT_MAX_REQUESTS:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Rate limit exceeded: max {RATE_LIMIT_MAX_REQUESTS} requests per "
                f"{RATE_LIMIT_WINDOW_SECONDS}s. Try again shortly."
            ),
        )
    recent.append(now)
    _request_log[ip] = recent


class DecisionRequest(BaseModel):
    question: str
    context: dict = {}


@router.post("/ask")
async def ask(req: DecisionRequest, request: Request):
    """Send a natural-language supply chain question to the decision agent.

    Returns the agent's full structured result — answer, the SQL it ran to get
    there, and extracted action items — not just the answer text. An earlier
    version collapsed this to {"answer": ...}, which silently discarded
    action_items even though the agent always computes them (see
    agent/decision_agent.py's _extract_action_items).

    Rate-limited per IP (see _check_rate_limit) — this endpoint calls the real
    Anthropic API with no auth in front of it, so an unbounded public endpoint is
    a real spend risk, not a theoretical one. agent/decision_agent.py separately
    caps turns per request (MAX_TURNS) and the overall request duration
    (REQUEST_TIMEOUT_SECONDS) — this endpoint only bounds request frequency.
    """
    _check_rate_limit(_client_ip(request))
    return await run_decision_agent(req.question, req.context)
