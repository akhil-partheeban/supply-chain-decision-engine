"""
Export the data layer to S3 (or, for local testing, to a plain local directory —
the same code path handles both, see below).

Three things get exported, because they serve three different consumers:

  1. Bronze/silver/gold tables, as Parquet, one object per table under
     <dest>/<layer>/<table>.parquet. This is the actual "data lake" artifact — Parquet
     in S3 is queryable directly by Athena, Redshift Spectrum, or another DuckDB
     process via `read_parquet('s3://...')`, without needing this project's code at
     all. This is what "deploy the data layer to S3" means in a lakehouse sense.

  2. A single-file DuckDB snapshot (the whole .duckdb database), uploaded as one
     object. This is what the containerized API fetches on cold start (see
     docker/fetch_db.py) — pulling one file over the network at container startup is
     far simpler than having the API reconstruct query patterns against remote
     Parquet, at the cost of the API only ever seeing data as fresh as the last
     snapshot upload.

  3. The raw Olist CSVs, uploaded as-is. Optional, off by default — see the --raw-csvs
     flag. Mirrors data/raw/ into S3 so bronze ingestion could, in principle, run
     from an S3-hosted source instead of a local checkout with the Kaggle download
     already in place.

Usage:
    # Parquet export + DuckDB snapshot, to a local directory (no AWS needed — good for
    # testing the export logic itself):
    python -m scripts.export_to_s3 --dest /tmp/export_test

    # The real thing:
    python -m scripts.export_to_s3 --dest s3://my-bucket/supply-chain --raw-csvs

Requires AWS credentials in the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
/ AWS_REGION, or a configured profile) when --dest is an s3:// URI. No credentials are
needed for a local --dest.
"""

import argparse
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold")


def _is_s3(dest: str) -> bool:
    return urlparse(dest).scheme == "s3"


def _configure_httpfs(conn: duckdb.DuckDBPyConnection) -> None:
    """Load DuckDB's httpfs extension and point it at AWS credentials from the
    environment. DuckDB speaks S3's HTTP API directly — no boto3 needed for this
    part, which is why the Parquet export path and the local-directory export path
    can share one `COPY ... TO '<dest>'` statement regardless of destination.
    """
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    region = os.getenv("AWS_REGION", "us-east-1")
    conn.execute(f"SET s3_region='{region}'")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        conn.execute(f"SET s3_access_key_id='{access_key}'")
        conn.execute(f"SET s3_secret_access_key='{secret_key}'")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    if session_token:
        conn.execute(f"SET s3_session_token='{session_token}'")


def export_parquet(conn: duckdb.DuckDBPyConnection, dest: str) -> dict[str, list[str]]:
    exported: dict[str, list[str]] = {}
    for schema in LAYERS:
        tables = [
            r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY 1",
                [schema],
            ).fetchall()
        ]
        exported[schema] = []
        if not _is_s3(dest):
            Path(f"{dest.rstrip('/')}/{schema}").mkdir(parents=True, exist_ok=True)
        for table in tables:
            target = f"{dest.rstrip('/')}/{schema}/{table}.parquet"
            conn.execute(f'COPY "{schema}"."{table}" TO \'{target}\' (FORMAT PARQUET)')
            log.info("  %s.%-35s -> %s", schema, table, target)
            exported[schema].append(target)
    return exported


def export_db_snapshot(db_path: str, dest: str) -> str:
    """Upload the whole .duckdb file as one object — see module docstring, item 2."""
    filename = Path(db_path).name
    target = f"{dest.rstrip('/')}/{filename}"

    if _is_s3(target):
        import boto3
        parsed = urlparse(target)
        bucket, key = parsed.netloc, parsed.path.lstrip("/")
        boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1")).upload_file(
            db_path, bucket, key
        )
    else:
        import shutil
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(db_path, target)

    size_mb = Path(db_path).stat().st_size / (1024 * 1024)
    log.info("  DuckDB snapshot (%.1f MB) -> %s", size_mb, target)
    return target


def export_raw_csvs(raw_dir: str, dest: str) -> list[str]:
    raw_path = Path(raw_dir)
    csv_files = sorted(raw_path.glob("*.csv"))
    if not csv_files:
        log.warning("No CSVs found in %s — nothing to upload", raw_dir)
        return []

    uploaded = []
    if _is_s3(dest):
        import boto3
        parsed = urlparse(dest)
        bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
        s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
        for f in csv_files:
            key = f"{prefix.rstrip('/')}/raw/{f.name}"
            s3.upload_file(str(f), bucket, key)
            uploaded.append(f"s3://{bucket}/{key}")
    else:
        import shutil
        out_dir = Path(dest) / "raw"
        out_dir.mkdir(parents=True, exist_ok=True)
        for f in csv_files:
            shutil.copy(f, out_dir / f.name)
            uploaded.append(str(out_dir / f.name))

    log.info("  Uploaded %d raw CSVs -> %s/raw/", len(uploaded), dest.rstrip("/"))
    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the data layer to S3 (or a local directory)")
    parser.add_argument("--db", default=os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb"))
    parser.add_argument("--dest", required=True, help="s3://bucket/prefix or a local directory path")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--raw-csvs", action="store_true", help="Also upload raw CSVs (off by default)")
    parser.add_argument("--skip-parquet", action="store_true", help="Skip the per-table Parquet export")
    parser.add_argument("--skip-snapshot", action="store_true", help="Skip the whole-file DuckDB snapshot upload")
    args = parser.parse_args()

    if not Path(args.db).exists():
        raise SystemExit(f"DuckDB file not found at {args.db} — run ingestion + dbt build first.")

    conn = duckdb.connect(args.db, read_only=True)
    if _is_s3(args.dest):
        _configure_httpfs(conn)

    if not args.skip_parquet:
        log.info("Exporting bronze/silver/gold tables as Parquet to %s ...", args.dest)
        export_parquet(conn, args.dest)

    conn.close()

    if not args.skip_snapshot:
        log.info("Uploading DuckDB snapshot ...")
        export_db_snapshot(args.db, args.dest)

    if args.raw_csvs:
        log.info("Uploading raw CSVs ...")
        export_raw_csvs(args.raw_dir, args.dest)

    log.info("Export complete -> %s", args.dest)


if __name__ == "__main__":
    main()
