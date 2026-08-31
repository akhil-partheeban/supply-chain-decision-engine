"""
Container startup hook: pull the gold-layer DuckDB snapshot from S3 before uvicorn
starts, if one is configured.

Why this exists: AWS App Runner and ECS Fargate containers have an ephemeral
filesystem — nothing written to disk survives a redeploy or a scale-out event, and a
fresh container starts with none of the data baked in unless it was part of the image
itself. Baking the multi-hundred-MB DuckDB file into the image would mean rebuilding
and re-pushing the image every time the data refreshes. Instead, the data-refresh job
(scripts/export_to_s3.py) uploads a fresh snapshot to S3, and every container fetches
the current snapshot once at cold start.

If DUCKDB_S3_URI is not set, this is a no-op — local development and the existing
`docker compose up` workflow (which bind-mounts data/ from the host) are completely
unaffected. This is what "keep the existing setup working" means concretely here: the
S3 fetch path only activates when explicitly configured for a cloud deployment.
"""

import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def fetch_db(s3_uri: str, dest_path: str) -> None:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"DUCKDB_S3_URI must be an s3:// URI, got: {s3_uri}")

    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    log.info("Fetching DuckDB snapshot: s3://%s/%s -> %s", bucket, key, dest)
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    s3.download_file(bucket, key, str(dest))
    size_mb = dest.stat().st_size / (1024 * 1024)
    log.info("Fetched %.1f MB", size_mb)


def main() -> None:
    s3_uri = os.getenv("DUCKDB_S3_URI")
    db_path = os.getenv("DUCKDB_PATH", "data/duckdb/supply_chain.duckdb")

    if not s3_uri:
        log.info("DUCKDB_S3_URI not set — skipping S3 fetch, using local file at %s", db_path)
        if not Path(db_path).exists():
            log.warning(
                "%s does not exist and no DUCKDB_S3_URI was provided — "
                "the API will fail to serve requests until a database is available.",
                db_path,
            )
        return

    try:
        fetch_db(s3_uri, db_path)
    except Exception:
        log.exception("Failed to fetch DuckDB snapshot from %s", s3_uri)
        # Fail loudly rather than silently serving stale or missing data — an API
        # container with no data is a container that should not pass its health check.
        sys.exit(1)


if __name__ == "__main__":
    main()
