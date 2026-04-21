"""Supplier performance endpoints."""

import os

import duckdb
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()


def get_conn() -> duckdb.DuckDBPyConnection:
    db_path = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")
    return duckdb.connect(db_path, read_only=True)


@router.get("/")
def list_suppliers(limit: int = Query(50, le=500)):
    """Return top suppliers ranked by avg review score."""
    try:
        conn = get_conn()
        rows = conn.execute(
            f"""
            SELECT *
            FROM gold.gold_supplier_performance
            ORDER BY avg_review_score DESC NULLS LAST
            LIMIT {limit}
            """
        ).fetchdf()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return rows.to_dict(orient="records")


@router.get("/{seller_id}")
def get_supplier(seller_id: str):
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM gold.gold_supplier_performance WHERE seller_id = ?",
            [seller_id],
        ).fetchdf()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if row.empty:
        raise HTTPException(status_code=404, detail="Supplier not found")
    return row.iloc[0].to_dict()
