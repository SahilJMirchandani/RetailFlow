
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_DIR = PROJECT_ROOT / "data" / "raw" / "olist"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "olist"
MANIFEST_FILE = (
    PROJECT_ROOT / "data" / "output" / "olist_bronze_manifest.json"
)

CHUNK_SIZE = 100_000

# Expected columns from the public Olist dataset.
EXPECTED_COLUMNS = {
    "olist_customers_dataset.csv": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "olist_geolocation_dataset.csv": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],
    "olist_order_items_dataset.csv": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],
    "olist_order_payments_dataset.csv": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],
    "olist_order_reviews_dataset.csv": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "olist_orders_dataset.csv": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_products_dataset.csv": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    "olist_sellers_dataset.csv": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "product_category_name_translation.csv": [
        "product_category_name",
        "product_category_name_english",
    ],
}


def sha256_file(path):
    """Calculate a SHA-256 checksum without loading the whole file."""

    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def ingest_csv_to_parquet(source_path, output_path):
    """Convert a CSV to Parquet using bounded-size chunks."""

    temp_path = output_path.with_name(
        output_path.name + ".tmp"
    )

    total_rows = 0
    total_blank_cells = 0
    columns = []
    writer = None

    try:
        # Preserve raw field values; blank fields remain empty strings.
        chunks = pd.read_csv(
            source_path,
            dtype=str,
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
            encoding="utf-8-sig",
            on_bad_lines="error",
        )

        for chunk in chunks:
            if not columns:
                columns = chunk.columns.tolist()

            total_blank_cells += int(
                chunk.eq("").to_numpy().sum()
            )

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False,
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    temp_path,
                    table.schema,
                    compression="snappy",
                )

            writer.write_table(table)
            total_rows += len(chunk)

        if writer is None:
            raise ValueError("CSV has no data rows.")

        writer.close()
        writer = None

        # Replace the completed output only after a successful write.
        temp_path.replace(output_path)

    except Exception:
        if writer is not None:
            writer.close()

        temp_path.unlink(missing_ok=True)
        raise

    return {
        "row_count": total_rows,
        "column_count": len(columns),
        "columns": columns,
        "blank_cells": total_blank_cells,
    }


def ingest_olist():
    """Ingest all nine Olist source tables into Bronze Parquet."""

    print("=" * 70)
    print("RetailFlow 2.0 - Olist Bronze Ingestion")
    print("=" * 70)

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Olist source directory not found: {SOURCE_DIR}"
        )

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)

    ingestion_time = datetime.now(timezone.utc).isoformat()
    table_results = []
    failures = []

    for filename, expected_columns in EXPECTED_COLUMNS.items():
        source_path = SOURCE_DIR / filename
        output_path = BRONZE_DIR / f"{source_path.stem}.parquet"

        print(f"\nProcessing: {filename}")

        if not source_path.is_file():
            failures.append({
                "file": filename,
                "error": "Source file is missing.",
            })
            print("  FAILED: Source file missing.")
            continue

        if source_path.stat().st_size == 0:
            failures.append({
                "file": filename,
                "error": "Source file is empty.",
            })
            print("  FAILED: Source file is empty.")
            continue

        try:
            stats = ingest_csv_to_parquet(
                source_path,
                output_path,
            )

            missing_columns = [
                column
                for column in expected_columns
                if column not in stats["columns"]
            ]

            schema_valid = not missing_columns

            result = {
                "source_file": filename,
                "source_size_bytes": source_path.stat().st_size,
                "source_sha256": sha256_file(source_path),
                "bronze_file": output_path.relative_to(
                    PROJECT_ROOT
                ).as_posix(),
                "bronze_size_bytes": output_path.stat().st_size,
                "row_count": stats["row_count"],
                "column_count": stats["column_count"],
                "columns": stats["columns"],
                "blank_cells": stats["blank_cells"],
                "expected_schema_valid": schema_valid,
                "missing_expected_columns": missing_columns,
                "ingested_at_utc": ingestion_time,
                "status": (
                    "success"
                    if schema_valid and stats["row_count"] > 0
                    else "quality_check_failed"
                ),
            }

            table_results.append(result)

            print(f"  Rows          : {stats['row_count']:,}")
            print(f"  Columns       : {stats['column_count']}")
            print(f"  Blank cells   : {stats['blank_cells']:,}")
            print(f"  Schema valid  : {schema_valid}")
            print(f"  Bronze output : {output_path}")

            if result["status"] != "success":
                failures.append({
                    "file": filename,
                    "error": (
                        "Empty data or missing expected columns: "
                        f"{missing_columns}"
                    ),
                })

        except Exception as error:
            failures.append({
                "file": filename,
                "error": str(error),
            })
            print(f"  FAILED: {error}")

    manifest = {
        "project": "RetailFlow",
        "dataset": "Olist Brazilian E-Commerce",
        "source_directory": SOURCE_DIR.relative_to(
            PROJECT_ROOT
        ).as_posix(),
        "bronze_directory": BRONZE_DIR.relative_to(
            PROJECT_ROOT
        ).as_posix(),
        "ingestion_started_at_utc": ingestion_time,
        "expected_table_count": len(EXPECTED_COLUMNS),
        "tables_processed": len(table_results),
        "tables_succeeded": sum(
            result["status"] == "success"
            for result in table_results
        ),
        "tables_failed": len(failures),
        "tables": table_results,
        "failures": failures,
        "success": (
            len(table_results) == len(EXPECTED_COLUMNS)
            and not failures
        ),
    }

    MANIFEST_FILE.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print("\n" + "=" * 70)
    print("BRONZE INGESTION SUMMARY")
    print("=" * 70)
    print(f"Tables processed : {manifest['tables_processed']}/9")
    print(f"Tables succeeded : {manifest['tables_succeeded']}/9")
    print(f"Tables failed    : {manifest['tables_failed']}")
    print(f"Manifest         : {MANIFEST_FILE}")

    if not manifest["success"]:
        print("\nBronze ingestion completed with errors.")
        return False

    print("\nAll nine Olist tables passed the ingestion checks.")
    return True


if __name__ == "__main__":
    # cspell:ignore olist
    try:
        success = ingest_olist()
    except Exception as error:
        print(f"\nINGESTION ERROR: {error}")
        success = False

    if not success:
        sys.exit(1)
