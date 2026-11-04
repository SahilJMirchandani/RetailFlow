"""Local unit tests for the optional AWS data-lake integration (no AWS calls)."""
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aws.config import load_aws_settings, require_aws_settings
from aws.data_lake import (
    arrow_type_to_glue,
    build_glue_table_input,
    collect_publish_files,
    s3_key_for_file,
)
from aws.athena_query import validate_read_only_sql


def test_aws_settings_disabled_by_default(monkeypatch):
    monkeypatch.delenv("RETAILFLOW_AWS_ENABLED", raising=False)
    settings = load_aws_settings()
    assert settings["enabled"] is False


def test_require_aws_settings_requires_existing_bucket(monkeypatch):
    monkeypatch.setenv("RETAILFLOW_AWS_ENABLED", "true")
    monkeypatch.delenv("RETAILFLOW_AWS_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="RETAILFLOW_AWS_BUCKET"):
        require_aws_settings()


def test_require_aws_settings_rejects_invalid_bucket(monkeypatch):
    monkeypatch.setenv("RETAILFLOW_AWS_ENABLED", "true")
    monkeypatch.setenv("RETAILFLOW_AWS_BUCKET", "Not_A_Valid_Bucket")
    with pytest.raises(ValueError, match="valid S3 bucket"):
        require_aws_settings()


def test_s3_key_for_gold_table_uses_separate_table_prefix(tmp_path):
    path = tmp_path / "data" / "gold" / "olist" / "gold_daily_sales.parquet"
    expected = "retailflow/gold/olist/gold_daily_sales/part-00000.parquet"
    assert s3_key_for_file(path, tmp_path, "retailflow") == expected


def test_s3_key_for_metadata_uses_metadata_prefix(tmp_path):
    path = tmp_path / "data" / "output" / "olist_gold_quality.json"
    assert (
        s3_key_for_file(path, tmp_path, "retailflow")
        == "retailflow/metadata/olist_gold_quality.json"
    )


def test_s3_key_rejects_raw_inputs(tmp_path):
    path = tmp_path / "data" / "raw" / "olist" / "olist_orders_dataset.csv"
    with pytest.raises(ValueError, match="outside"):
        s3_key_for_file(path, tmp_path, "retailflow")


def test_collect_publish_files_excludes_raw_and_images_by_default(tmp_path):
    (tmp_path / "data" / "raw" / "olist").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "olist" / "private.csv").write_text("a\n1\n")
    (tmp_path / "data" / "bronze" / "olist").mkdir(parents=True)
    bronze = tmp_path / "data" / "bronze" / "olist" / "orders.parquet"
    bronze.write_bytes(b"not-a-real-parquet-file")
    (tmp_path / "data" / "catalog_images" / "dummyjson").mkdir(parents=True)
    image = tmp_path / "data" / "catalog_images" / "dummyjson" / "1.jpg"
    image.write_bytes(b"image")
    (tmp_path / "data" / "output").mkdir(parents=True)
    report = tmp_path / "data" / "output" / "olist_gold_quality.json"
    report.write_text(json.dumps({"passed": True}), encoding="utf-8")

    selected = collect_publish_files(tmp_path)
    assert bronze in selected
    assert report in selected
    assert image not in selected
    assert all("raw" not in path.parts for path in selected)


def test_collect_publish_files_can_include_images_when_explicit(tmp_path):
    image_dir = tmp_path / "data" / "catalog_images"
    image_dir.mkdir(parents=True)
    image = image_dir / "sample.jpg"
    image.write_bytes(b"image")
    assert image in collect_publish_files(tmp_path, upload_images=True)


def test_arrow_types_map_to_glue_types():
    assert arrow_type_to_glue(pa.string()) == "string"
    assert arrow_type_to_glue(pa.int64()) == "bigint"
    assert arrow_type_to_glue(pa.float64()) == "double"
    assert arrow_type_to_glue(pa.bool_()) == "boolean"
    assert arrow_type_to_glue(pa.date32()) == "date"


def test_build_glue_table_input_reads_parquet_schema(tmp_path):
    parquet_path = tmp_path / "gold_daily_sales.parquet"
    table = pa.table({
        "order_date": pa.array(["2018-01-01"], type=pa.string()),
        "order_count": pa.array([12], type=pa.int64()),
    })
    pq.write_table(table, parquet_path)

    table_input = build_glue_table_input(
        "gold_daily_sales",
        parquet_path,
        "s3://retailflow-test/retailflow/gold/olist/gold_daily_sales/",
    )
    assert table_input["Name"] == "gold_daily_sales"
    assert table_input["StorageDescriptor"]["Location"].endswith("/")
    assert table_input["StorageDescriptor"]["Columns"] == [
        {"Name": "order_date", "Type": "string"},
        {"Name": "order_count", "Type": "bigint"},
    ]


def test_athena_query_accepts_read_only_sql():
    assert validate_read_only_sql("SELECT * FROM gold_daily_sales LIMIT 10") == (
        "SELECT * FROM gold_daily_sales LIMIT 10"
    )
    assert validate_read_only_sql("SHOW TABLES;") == "SHOW TABLES"


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE gold_daily_sales",
        "CREATE TABLE unsafe AS SELECT 1",
        "SELECT 1; DROP TABLE gold_daily_sales",
        "SELECT 1 -- comment",
    ],
)
def test_athena_query_rejects_unsafe_or_multiple_statements(sql):
    with pytest.raises(ValueError):
        validate_read_only_sql(sql)
