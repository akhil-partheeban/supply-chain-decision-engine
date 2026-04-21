"""
LangChain agent that answers supply chain questions by querying DuckDB.

The agent is given a DuckDB SQL tool and can introspect schemas,
run analytical queries, and synthesize recommendations.
"""

import os

import duckdb
from dotenv import load_dotenv
from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.tools import tool
from langchain_openai import ChatOpenAI

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

SYSTEM_PROMPT = """You are a supply chain analyst assistant with access to a DuckDB database
containing Olist e-commerce data (bronze/silver/gold schemas), UN Comtrade trade flow data,
and World Bank LPI scores.

Use the run_sql tool to query the database and answer questions about:
- Supplier performance and reliability
- Delivery time analysis
- Trade flow patterns
- Logistics performance by country

Always ground your answers in data. When you write SQL, prefer querying gold-layer tables first,
then silver, then bronze only for raw exploration."""


@tool
def run_sql(query: str) -> str:
    """Execute a read-only SQL query against the supply chain DuckDB database and return results as text."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        if df.empty:
            return "Query returned no rows."
        return df.to_string(index=False, max_rows=50)
    except Exception as exc:
        return f"SQL error: {exc}"


@tool
def list_tables(schema: str = "gold") -> str:
    """List all tables in a given schema (bronze, silver, or gold)."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [schema],
        ).fetchall()
        conn.close()
        return "\n".join(r[0] for r in rows) or f"No tables found in schema '{schema}'"
    except Exception as exc:
        return f"Error: {exc}"


def build_agent() -> AgentExecutor:
    llm = ChatOpenAI(model=MODEL, temperature=0)
    tools = [run_sql, list_tables]
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", "{input}"),
            MessagesPlaceholder("agent_scratchpad"),
        ]
    )
    agent = create_openai_functions_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True, max_iterations=10)


async def run_decision_agent(question: str, context: dict = {}) -> str:
    executor = build_agent()
    full_input = question
    if context:
        full_input += f"\n\nAdditional context: {context}"
    result = await executor.ainvoke({"input": full_input})
    return result.get("output", "")
