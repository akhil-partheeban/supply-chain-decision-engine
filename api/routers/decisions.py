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
    """Send a natural-language supply chain question to the LangChain agent."""
    result = await run_decision_agent(req.question, req.context)
    return {"answer": result}
