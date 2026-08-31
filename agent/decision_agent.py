"""
Claude-powered supply chain decision agent.

Uses the Anthropic SDK directly with a manual tool loop.
ask(question) → {"answer": str, "sql_used": list[str], "action_items": list[str]}
"""

import json
import os

import anthropic
import duckdb
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")
MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """You are a supply chain analyst assistant with access to a DuckDB database
containing Olist e-commerce data.

Gold-layer tables (query these first):
  gold.gold_supplier_scorecard      — per-seller reliability_score, risk_tier (HIGH/MEDIUM/LOW),
                                       delivery_days_stddev, late_delivery_rate, avg_review_score,
                                       revenue, freight cost
  gold.gold_concentration_risk      — per-seller revenue_share_pct, hhi_contribution, concentration_flag
                                       (single-supplier dependency risk)
  gold.gold_geo_concentration       — per-state revenue share and concentration_flag (geographic risk)
  gold.gold_sourcing_cost_drivers   — per-product-category freight_pct_of_spend and freight_burden_tier
  gold.gold_executive_summary       — single-row KPI rollup (includes hhi_index, hhi_interpretation)

Silver-layer tables: silver.silver_orders, silver.silver_order_items, silver.silver_sellers

Always query gold tables first, silver next, bronze only for raw exploration.
Be concise and data-driven. End every response with 2-4 concrete action items prefixed with "ACTION:".
"""

TOOLS = [
    {
        "name": "run_sql",
        "description": "Execute a read-only SQL query against the supply chain DuckDB database and return results as text.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The SQL query to execute.",
                }
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_executive_summary",
        "description": "Return the single-row executive KPI summary from gold.gold_executive_summary (overall late rate, % high-risk sellers, avg review score, total orders/sellers). Call this for any high-level health or summary question.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


# ── Tool implementations ───────────────────────────────────────────────────────

def _run_sql(query: str) -> str:
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        if df.empty:
            return "Query returned no rows."
        return df.to_string(index=False, max_rows=50)
    except Exception as exc:
        return f"SQL error: {exc}"


def _get_executive_summary() -> str:
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute("SELECT * FROM gold.gold_executive_summary").fetchdf()
        conn.close()
        if df.empty:
            return "Executive summary table is empty."
        row = df.iloc[0]
        return (
            f"Total orders: {int(row['total_orders']):,}\n"
            f"Total suppliers scored: {int(row['total_suppliers']):,}\n"
            f"Total revenue: ${row['total_revenue']:,.2f}\n"
            f"Overall late delivery rate: {row['overall_late_rate']:.1%}\n"
            f"Average reliability score: {row['avg_reliability_score']:.1f} / 100\n"
            f"% high-risk suppliers: {row['pct_high_risk_suppliers']:.1%}\n"
            f"Average review score: {row['avg_review_score']:.2f} / 5.00\n"
            f"Supplier concentration (HHI): {row['hhi_index']:.1f} ({row['hhi_interpretation']})\n"
            f"Top-5 supplier revenue share: {row['top5_supplier_revenue_share_pct']:.1f}%"
        )
    except Exception as exc:
        return f"Error: {exc}"


def _dispatch(tool_name: str, tool_input: dict) -> str:
    if tool_name == "run_sql":
        return _run_sql(tool_input["query"])
    if tool_name == "get_executive_summary":
        return _get_executive_summary()
    return f"Unknown tool: {tool_name}"


# ── Action-item extraction ─────────────────────────────────────────────────────

def _extract_action_items(answer: str) -> list[str]:
    """Pull ACTION: lines from the answer, or ask Claude to generate them."""
    lines = [
        line.strip().lstrip("•-").strip()
        for line in answer.splitlines()
        if line.strip().upper().startswith("ACTION:")
    ]
    items = [line[7:].strip() if line.upper().startswith("ACTION:") else line for line in lines]
    if items:
        return items[:4]

    # Fallback: ask Claude to extract them
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=512,
        system="Extract 2-4 concrete supply chain action items from the text. Return ONLY a JSON array of strings, no other text.",
        messages=[{"role": "user", "content": answer}],
    )
    try:
        text = resp.content[0].text.strip()
        if text.startswith("["):
            return json.loads(text)
    except Exception:
        pass
    return ["Review findings and prioritize remediation steps."]


# ── Main agent loop ────────────────────────────────────────────────────────────

def ask(question: str) -> dict:
    """
    Ask a natural-language supply chain question.

    Returns:
        {
            "answer": str,
            "sql_used": list[str],
            "action_items": list[str],
        }
    """
    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": question}]
    sql_used: list[str] = []

    while True:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Append assistant turn
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                if block.name == "run_sql":
                    sql_used.append(block.input.get("query", ""))
                result = _dispatch(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })
            messages.append({"role": "user", "content": tool_results})
        else:
            break  # unexpected stop_reason — bail out

    # Extract final text answer
    answer = next(
        (block.text for block in response.content if hasattr(block, "text")),
        "",
    )
    action_items = _extract_action_items(answer)

    return {
        "answer": answer,
        "sql_used": sql_used,
        "action_items": action_items,
    }


# ── Async shim used by api/routers/decisions.py ────────────────────────────────

async def run_decision_agent(question: str, context: dict | None = None) -> dict:
    """Returns the full {"answer", "sql_used", "action_items"} dict from ask() —
    this used to discard everything except "answer", which meant action_items (the
    whole point of the ACTION: extraction in _extract_action_items) never reached
    the API response or the dashboard. See DECISIONS.md, Phase 4."""
    full_input = question
    if context:
        full_input += f"\n\nAdditional context: {context}"
    return ask(full_input)
