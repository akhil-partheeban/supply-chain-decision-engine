"""AI-assisted supply chain decision endpoints."""

from fastapi import APIRouter
from pydantic import BaseModel

from agent.decision_agent import run_decision_agent

router = APIRouter()


class DecisionRequest(BaseModel):
    question: str
    context: dict = {}


@router.post("/ask")
async def ask(req: DecisionRequest):
    """Send a natural-language supply chain question to the decision agent.

    Returns the agent's full structured result — answer, the SQL it ran to get
    there, and extracted action items — not just the answer text. An earlier
    version collapsed this to {"answer": ...}, which silently discarded
    action_items even though the agent always computes them (see
    agent/decision_agent.py's _extract_action_items).
    """
    return await run_decision_agent(req.question, req.context)
