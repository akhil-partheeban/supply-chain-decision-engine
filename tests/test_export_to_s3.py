"""
Tests for scripts/export_to_s3.py.

The Parquet export path (export_parquet) uses DuckDB's own httpfs extension to talk
to S3 directly — it does not go through boto3, so moto (which mocks boto3 calls)
can't intercept it. That path is instead verified via the local-directory
destination, which runs the exact same `COPY ... TO '<dest>'` SQL, just pointed at a
filesystem path instead of an s3:// URI — see test_export_parquet_local_dir.

The snapshot and raw-CSV upload paths DO go through boto3 (see export_db_snapshot /
export_raw_csvs), so those are tested against a moto-mocked S3 bucket, which
exercises the real boto3 call path without touching AWS.
"""

import duckdb
import pytest
from moto import mock_aws

from scripts.export_to_s3 import export_db_snapshot, export_parquet, export_raw_csvs


@pytest.fixture()
def sample_db(tmp_path):
    db_path = str(tmp_path / "sample.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA bronze")
    conn.execute("CREATE SCHEMA gold")
    conn.execute("CREATE TABLE bronze.orders AS SELECT * FROM (VALUES (1), (2), (3)) t(id)")
    conn.execute("CREATE TABLE gold.gold_executive_summary AS SELECT * FROM (VALUES (1)) t(x)")
    conn.close()
    return db_path


def test_export_parquet_local_dir(sample_db, tmp_path):
    dest = str(tmp_path / "export")
    conn = duckdb.connect(sample_db, read_only=True)
    exported = export_parquet(conn, dest)
    conn.close()

    assert exported["bronze"] == [f"{dest}/bronze/orders.parquet"]
    assert exported["gold"] == [f"{dest}/gold/gold_executive_summary.parquet"]

    # Round-trip: read the exported Parquet back and confirm row count matches.
    check = duckdb.connect()
    n = check.execute(f"SELECT count(*) FROM read_parquet('{dest}/bronze/orders.parquet')").fetchone()[0]
    assert n == 3


@mock_aws
def test_export_db_snapshot_to_s3(sample_db):
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    target = export_db_snapshot(sample_db, "s3://test-bucket/prefix")

    assert target == "s3://test-bucket/prefix/sample.duckdb"
    obj = s3.get_object(Bucket="test-bucket", Key="prefix/sample.duckdb")
    assert obj["ContentLength"] > 0


@mock_aws
def test_export_raw_csvs_to_s3(tmp_path):
    import boto3
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "a.csv").write_text("id\n1\n2\n")
    (raw_dir / "b.csv").write_text("id\n3\n")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    uploaded = export_raw_csvs(str(raw_dir), "s3://test-bucket/prefix")

    assert sorted(uploaded) == [
        "s3://test-bucket/prefix/raw/a.csv",
        "s3://test-bucket/prefix/raw/b.csv",
    ]
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket="test-bucket")["Contents"]}
    assert keys == {"prefix/raw/a.csv", "prefix/raw/b.csv"}


def test_export_raw_csvs_local_dir(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "a.csv").write_text("id\n1\n")

    dest = tmp_path / "export"
    uploaded = export_raw_csvs(str(raw_dir), str(dest))

    assert uploaded == [str(dest / "raw" / "a.csv")]
    assert (dest / "raw" / "a.csv").exists()
