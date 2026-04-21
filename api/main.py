"""FastAPI application — supply chain decision engine."""

import os

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.routers import suppliers, decisions

app = FastAPI(
    title="Supply Chain Decision Engine",
    version="0.1.0",
    description="Query supply chain analytics and trigger AI-assisted decisions.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(suppliers.router, prefix="/suppliers", tags=["suppliers"])
app.include_router(decisions.router, prefix="/decisions", tags=["decisions"])


@app.get("/health")
def health():
    db_path = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")
    try:
        conn = duckdb.connect(db_path, read_only=True)
        conn.execute("SELECT 1").fetchone()
        conn.close()
        db_status = "ok"
    except Exception as exc:
        db_status = str(exc)
    return {"status": "ok", "db": db_status}
