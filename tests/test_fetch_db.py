"""Tests for docker/fetch_db.py — the container-startup S3 fetch hook."""

import sys
from pathlib import Path

import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).parent.parent / "docker"))

from fetch_db import fetch_db


@mock_aws
def test_fetch_db_downloads_from_s3(tmp_path):
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="supply_chain.duckdb", Body=b"fake duckdb bytes")

    dest = tmp_path / "nested" / "dir" / "supply_chain.duckdb"
    fetch_db("s3://test-bucket/supply_chain.duckdb", str(dest))

    assert dest.exists()
    assert dest.read_bytes() == b"fake duckdb bytes"


def test_fetch_db_rejects_non_s3_uri(tmp_path):
    with pytest.raises(ValueError, match="s3://"):
        fetch_db("https://example.com/db.duckdb", str(tmp_path / "out.duckdb"))
