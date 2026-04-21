"""
Smoke-test for the LangChain/Claude decision agent.

Runs three real questions against the gold layer and prints structured output.
Requires ANTHROPIC_API_KEY in .env.
"""

import json
import sys
import os

# Run from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.decision_agent import ask

QUESTIONS = [
    "Which sellers are highest risk?",
    "Where is our concentration risk?",
    "Give me an executive summary of supply chain health",
]


def test_agent_questions():
    for question in QUESTIONS:
        print(f"\n{'='*70}")
        print(f"Q: {question}")
        print("=" * 70)

        result = ask(question)

        print(f"\n📋 ANSWER:\n{result['answer']}")

        if result["sql_used"]:
            print(f"\n🔍 SQL USED ({len(result['sql_used'])} queries):")
            for i, sql in enumerate(result["sql_used"], 1):
                print(f"  [{i}] {sql.strip()[:120]}{'...' if len(sql.strip()) > 120 else ''}")

        print(f"\n✅ ACTION ITEMS:")
        for item in result["action_items"]:
            print(f"  • {item}")

        # Basic structural assertions
        assert isinstance(result["answer"], str) and len(result["answer"]) > 20
        assert isinstance(result["sql_used"], list)
        assert isinstance(result["action_items"], list) and len(result["action_items"]) >= 1

    print(f"\n{'='*70}")
    print("All 3 questions passed.")


if __name__ == "__main__":
    test_agent_questions()
