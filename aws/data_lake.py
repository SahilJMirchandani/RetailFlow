"""Publish RetailFlow 2.0 Parquet layers to S3 and register Gold tables in Glue.

The local files remain the source of truth. This module only uses an existing
S3 bucket; it never creates buckets, IAM roles, Glue jobs, or crawlers.
"""
from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from aws.config import ROOT, require_aws_settings

OUTPUT_DIR = ROOT / "data" / "output"
PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_publish_files(root: Path = ROOT, upload_images: bool = False) -> list[Path]:
    """Collect curated pipeline outputs; never include raw inputs or credentials."""
    selected: set[Path] = set()
    for layer in ("bronze", "silver", "gold"):
        folder = root / "data" / layer
        if folder.is_dir():
            selected.update(
                path for path in folder.rglob("*")
                if path.is_file() and not path.name.endswith((".tmp", ".part"))
            )

    output = root / "data" / "output"
    if output.is_dir():
        for path in output.iterdir():
            if path.is_file() and (
                path.name.endswith("_quality.json")
                or path.name.endswith("_manifest.json")
                or path.name.endswith("_manifest.csv")
            ):
                selected.add(path)

    lineage = output / "lineage"
    if lineage.is_dir():
        selected.update(
            path for path in lineage.iterdir()
            if path.is_file() and path.suffix in {".json", ".jsonl"}
        )

    if upload_images:
        image_folder = root / "data" / "catalog_images"
        if image_folder.is_dir():
            selected.update(path for path in image_folder.rglob("*") if path.is_file())

    return sorted(selected, key=lambda path: path.as_posix().lower())


def s3_key_for_file(path: Path, root: Path, prefix: str) -> str:
    relative = path.relative_to(root)
    parts = relative.parts

    # One stable directory per Gold table allows Glue/Athena to read its Parquet data.
    if (
        len(parts) >= 4 and parts[:3] == ("data", "gold", "olist")
        and path.suffix.lower() == ".parquet"
    ):
        return f"{prefix}/gold/olist/{path.stem.lower()}/part-00000.parquet"

    if len(parts) >= 2 and parts[0] == "data" and parts[1] in {"bronze", "silver", "gold"}:
        return "/".join((prefix, *parts[1:]))

    if len(parts) >= 3 and parts[:2] == ("data", "output"):
        tail = parts[2:]
        if tail and tail[0] == "lineage":
            return "/".join((prefix, "metadata", "lineage", *tail[1:]))
        return "/".join((prefix, "metadata", *tail))

    if len(parts) >= 2 and parts[:2] == ("data", "catalog_images"):
        return "/".join((prefix, *parts))

    raise ValueError(f"File is outside the RetailFlow publication scope: {path}")


def arrow_type_to_glue(dtype: pa.DataType) -> str:
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "string"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_integer(dtype):
        return "bigint" if dtype.bit_width > 32 else "int"
    if pa.types.is_floating(dtype):
        return "double" if dtype.bit_width == 64 else "float"
    if pa.types.is_decimal(dtype):
        return f"decimal({dtype.precision},{dtype.scale})"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "binary"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return f"array<{arrow_type_to_glue(dtype.value_type)}>"
    raise TypeError(f"Unsupported Parquet type for Glue Catalog: {dtype}")


def build_glue_table_input(table_name: str, parquet_path: Path, s3_location: str) -> dict:
    schema = pq.read_schema(parquet_path)
    columns = [
        {
            "Name": field.name.strip().lower().replace(" ", "_"),
            "Type": arrow_type_to_glue(field.type),
        }
        for field in schema
    ]
    return {
        "Name": table_name.lower(),
        "Description": f"RetailFlow 2.0 Olist Gold table {table_name}",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
            "retailflow:layer": "gold",
            "retailflow:source": "olist",
        },
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location.rstrip("/") + "/",
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,
            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {"serialization.format": "1"},
            },
        },
    }


def _is_not_found(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code", "")
    return code in {"EntityNotFoundException", "404", "NoSuchKey", "NotFound"}


def _upload_one(s3, bucket: str, path: Path, key: str) -> dict:
    digest = sha256_file(path)
    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
        if existing.get("Metadata", {}).get("sha256") == digest:
            return {"key": key, "status": "unchanged", "size_bytes": path.stat().st_size}
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if not (_is_not_found(error) or status in {403, 404}):
            raise

    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    s3.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": content_type,
            "ServerSideEncryption": "AES256",
            "Metadata": {"sha256": digest},
        },
    )
    head = s3.head_object(Bucket=bucket, Key=key)
    if int(head.get("ContentLength", -1)) != path.stat().st_size:
        raise RuntimeError(f"S3 size verification failed for {key}")
    return {"key": key, "status": "uploaded", "size_bytes": path.stat().st_size}


def _ensure_glue_database(glue, database: str, bucket_uri: str) -> None:
    try:
        glue.get_database(Name=database)
    except ClientError as error:
        if not _is_not_found(error):
            raise
        glue.create_database(
            DatabaseInput={
                "Name": database,
                "Description": "RetailFlow 2.0 Olist Gold analytical tables",
                "LocationUri": bucket_uri,
                "Parameters": {"project": "RetailFlow", "layer": "gold"},
            }
        )


def _register_glue_table(glue, database: str, table_input: dict) -> str:
    name = table_input["Name"]
    try:
        glue.get_table(DatabaseName=database, Name=name)
    except ClientError as error:
        if not _is_not_found(error):
            raise
        glue.create_table(DatabaseName=database, TableInput=table_input)
        return "created"
    glue.update_table(DatabaseName=database, TableInput=table_input)
    return "updated"


def publish_data_and_register(root: Path = ROOT) -> dict:
    """Upload current layers to an existing bucket and register Gold in Glue."""
    settings = require_aws_settings()
    profile = os.getenv("AWS_PROFILE")
    session_args = {"region_name": settings["region"]}
    if profile:
        session_args["profile_name"] = profile
    session = boto3.Session(**session_args)
    sts = session.client("sts")
    identity = sts.get_caller_identity()
    s3 = session.client("s3")
    glue = session.client("glue")

    # Fail fast on a wrong bucket or region before uploading any object.
    s3.head_bucket(Bucket=settings["bucket"])
    location = s3.get_bucket_location(Bucket=settings["bucket"]).get("LocationConstraint")
    bucket_region = location or "us-east-1"
    if bucket_region != settings["region"]:
        raise RuntimeError(
            f"Bucket is in {bucket_region}, but RETAILFLOW_AWS_REGION is {settings['region']}."
        )

    files = collect_publish_files(
        root=root, upload_images=settings["upload_images"]
    )
    if not files:
        raise RuntimeError("No generated RetailFlow files found. Run the V2 pipeline first.")

    prefix = settings["prefix"]
    results = []
    gold = []
    for path in files:
        key = s3_key_for_file(path, root, prefix)
        result = _upload_one(s3, settings["bucket"], path, key)
        result["local_path"] = path.relative_to(root).as_posix()
        results.append(result)
        if (
            path.parent == root / "data" / "gold" / "olist"
            and path.suffix.lower() == ".parquet"
        ):
            gold.append((path, key))

    if not gold:
        raise RuntimeError("No Olist Gold Parquet files found; Glue tables were not registered.")

    database = settings["database"]
    base_uri = f"s3://{settings['bucket']}/{prefix}/"
    _ensure_glue_database(glue, database, base_uri)

    table_results = []
    for path, key in gold:
        table_prefix = key.rsplit("/", 1)[0] + "/"
        table_input = build_glue_table_input(
            path.stem, path, f"s3://{settings['bucket']}/{table_prefix}"
        )
        operation = _register_glue_table(glue, database, table_input)
        table_results.append({"table": path.stem.lower(), "operation": operation, "s3_location": table_input["StorageDescriptor"]["Location"]})

    report = {
        "project": "RetailFlow 2.0",
        "status": "success",
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "region": settings["region"],
        "bucket": settings["bucket"],
        "prefix": prefix,
        "glue_database": database,
        "aws_identity_validated": bool(identity.get("Arn")),
        "files_uploaded": sum(item["status"] == "uploaded" for item in results),
        "files_unchanged": sum(item["status"] == "unchanged" for item in results),
        "files": results,
        "glue_tables": table_results,
        "athena_workgroup": settings["workgroup"],
        "athena_output_location": settings["athena_output"],
        "notes": [
            "Raw source datasets and .env credentials were not uploaded.",
            "Local Parquet files remain the source of truth.",
            "S3 server-side encryption with S3-managed keys (AES256) was requested for uploaded objects.",
        ],
    }
    report_path = OUTPUT_DIR / "aws_publication_latest.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    manifest_key = f"{prefix}/metadata/aws_publication_latest.json"
    s3.put_object(
        Bucket=settings["bucket"],
        Key=manifest_key,
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    report["s3_manifest_uri"] = f"s3://{settings['bucket']}/{manifest_key}"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def sync_lineage_to_s3(root: Path = ROOT) -> int:
    """Copy the just-written latest-run JSON and append-only JSONL history to S3."""
    settings = require_aws_settings()
    profile = os.getenv("AWS_PROFILE")
    session_args = {"region_name": settings["region"]}
    if profile:
        session_args["profile_name"] = profile
    s3 = boto3.Session(**session_args).client("s3")
    lineage_dir = root / "data" / "output" / "lineage"
    if not lineage_dir.is_dir():
        return 0
    count = 0
    for name in ("pipeline_run_latest.json", "pipeline_runs.jsonl"):
        path = lineage_dir / name
        if not path.is_file():
            continue
        key = f"{settings['prefix']}/metadata/lineage/{name}"
        _upload_one(s3, settings["bucket"], path, key)
        count += 1
    return count
