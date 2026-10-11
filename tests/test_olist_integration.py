"""Synthetic end-to-end tests for RetailFlow's Olist Bronze/Silver/Gold flow.

The test creates tiny CSV inputs in a temporary directory, runs the actual
ingestion and ETL functions, and never reads or overwrites local Olist data.
"""

import csv
import hashlib
import importlib
import json
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal


TABLE_ROWS = {
    "olist_customers_dataset.csv": [
        {
            "customer_id": "c001",
            "customer_unique_id": "u001",
            "customer_zip_code_prefix": "01001",
            "customer_city": "sao paulo",
            "customer_state": "sp",
        },
        {
            "customer_id": "c002",
            "customer_unique_id": "u002",
            "customer_zip_code_prefix": "20040",
            "customer_city": "rio de janeiro",
            "customer_state": "rj",
        },
    ],
    "olist_geolocation_dataset.csv": [
        {
            "geolocation_zip_code_prefix": "01001",
            "geolocation_lat": "-23.55",
            "geolocation_lng": "-46.63",
            "geolocation_city": "sao paulo",
            "geolocation_state": "sp",
        },
        {
            "geolocation_zip_code_prefix": "20040",
            "geolocation_lat": "-22.90",
            "geolocation_lng": "-43.20",
            "geolocation_city": "rio de janeiro",
            "geolocation_state": "rj",
        },
    ],
    "olist_order_items_dataset.csv": [
        {
            "order_id": "o001",
            "order_item_id": "1",
            "product_id": "p001",
            "seller_id": "s001",
            "shipping_limit_date": "2018-01-03 00:00:00",
            "price": "100.00",
            "freight_value": "10.00",
        },
        {
            "order_id": "o002",
            "order_item_id": "1",
            "product_id": "p002",
            "seller_id": "s002",
            "shipping_limit_date": "2018-01-04 00:00:00",
            "price": "200.00",
            "freight_value": "15.00",
        },
    ],
    "olist_order_payments_dataset.csv": [
        {
            "order_id": "o001",
            "payment_sequential": "1",
            "payment_type": "credit_card",
            "payment_installments": "1",
            "payment_value": "110.00",
        },
        {
            "order_id": "o002",
            "payment_sequential": "1",
            "payment_type": "boleto",
            "payment_installments": "1",
            "payment_value": "215.00",
        },
    ],
    "olist_order_reviews_dataset.csv": [
        {
            "review_id": "r001",
            "order_id": "o001",
            "review_score": "5",
            "review_comment_title": "Great",
            "review_comment_message": "Good product",
            "review_creation_date": "2018-01-05 00:00:00",
            "review_answer_timestamp": "2018-01-06 00:00:00",
        },
        {
            "review_id": "r002",
            "order_id": "o002",
            "review_score": "4",
            "review_comment_title": "Nice",
            "review_comment_message": "Arrived safely",
            "review_creation_date": "2018-01-06 00:00:00",
            "review_answer_timestamp": "2018-01-07 00:00:00",
        },
    ],
    "olist_orders_dataset.csv": [
        {
            "order_id": "o001",
            "customer_id": "c001",
            "order_status": "delivered",
            "order_purchase_timestamp": "2018-01-01 10:00:00",
            "order_approved_at": "2018-01-01 10:10:00",
            "order_delivered_carrier_date": "2018-01-02 08:00:00",
            "order_delivered_customer_date": "2018-01-04 14:00:00",
            "order_estimated_delivery_date": "2018-01-08 00:00:00",
        },
        {
            "order_id": "o002",
            "customer_id": "c002",
            "order_status": "delivered",
            "order_purchase_timestamp": "2018-01-02 11:30:00",
            "order_approved_at": "2018-01-02 11:45:00",
            "order_delivered_carrier_date": "2018-01-03 09:00:00",
            "order_delivered_customer_date": "2018-01-05 16:00:00",
            "order_estimated_delivery_date": "2018-01-09 00:00:00",
        },
    ],
    "olist_products_dataset.csv": [
        {
            "product_id": "p001",
            "product_category_name": "cama_mesa_banho",
            "product_name_lenght": "20",
            "product_description_lenght": "100",
            "product_photos_qty": "2",
            "product_weight_g": "500",
            "product_length_cm": "20",
            "product_height_cm": "10",
            "product_width_cm": "15",
        },
        {
            "product_id": "p002",
            "product_category_name": "beleza_saude",
            "product_name_lenght": "25",
            "product_description_lenght": "140",
            "product_photos_qty": "3",
            "product_weight_g": "700",
            "product_length_cm": "22",
            "product_height_cm": "12",
            "product_width_cm": "16",
        },
    ],
    "olist_sellers_dataset.csv": [
        {
            "seller_id": "s001",
            "seller_zip_code_prefix": "01001",
            "seller_city": "sao paulo",
            "seller_state": "sp",
        },
        {
            "seller_id": "s002",
            "seller_zip_code_prefix": "20040",
            "seller_city": "rio de janeiro",
            "seller_state": "rj",
        },
    ],
    "product_category_name_translation.csv": [
        {
            "product_category_name": "cama_mesa_banho",
            "product_category_name_english": "bed_bath_table",
        },
        {
            "product_category_name": "beleza_saude",
            "product_category_name_english": "health_beauty",
        },
    ],
}


GOLD_SORT_KEYS = {
    "dim_customers": ["customer_id"],
    "dim_products": ["product_id"],
    "dim_sellers": ["seller_id"],
    "dim_date": ["date_key"],
    "fact_orders": ["order_id"],
    "fact_order_items": ["order_id", "order_item_id"],
    "gold_daily_sales": ["order_date"],
    "gold_category_sales": ["product_category"],
    "gold_seller_performance": ["seller_id"],
}


def write_csv(path, rows):
    """Write a fixture table with a stable, explicit column order."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(rows[0].keys()),
        )
        writer.writeheader()
        writer.writerows(rows)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_sandbox(monkeypatch, tmp_path, duplicate_customer=False):
    """Point real ETL modules at isolated temporary folders and write fixtures."""

    bronze_module = importlib.import_module("ingestion.ingest_olist")
    silver_module = importlib.import_module("etl.transform_olist_silver")
    gold_module = importlib.import_module("etl.build_olist_gold")

    project_root = tmp_path / "sandbox"
    source_dir = project_root / "data" / "raw" / "olist"
    bronze_dir = project_root / "data" / "bronze" / "olist"
    silver_dir = project_root / "data" / "silver" / "olist"
    gold_dir = project_root / "data" / "gold" / "olist"
    output_dir = project_root / "data" / "output"

    for module in (bronze_module, silver_module, gold_module):
        monkeypatch.setattr(module, "PROJECT_ROOT", project_root)

    monkeypatch.setattr(bronze_module, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(bronze_module, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(
        bronze_module, "MANIFEST_FILE", output_dir / "olist_bronze_manifest.json"
    )

    monkeypatch.setattr(silver_module, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(silver_module, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(
        silver_module, "REPORT_FILE", output_dir / "olist_silver_quality.json"
    )

    monkeypatch.setattr(gold_module, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(gold_module, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(
        gold_module, "REPORT_FILE", output_dir / "olist_gold_quality.json"
    )

    for filename, rows in TABLE_ROWS.items():
        rows_to_write = [dict(row) for row in rows]

        if duplicate_customer and filename == "olist_customers_dataset.csv":
            # Keep the natural business identifier distinct while deliberately
            # duplicating the source primary key so Silver must reject it.
            duplicate = dict(rows_to_write[0])
            duplicate["customer_unique_id"] = "u003"
            rows_to_write.append(duplicate)

        write_csv(source_dir / filename, rows_to_write)

    return {
        "project_root": project_root,
        "source_dir": source_dir,
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
        "output_dir": output_dir,
        "bronze_module": bronze_module,
        "silver_module": silver_module,
        "gold_module": gold_module,
    }


def run_bronze_silver_gold(sandbox):
    """Execute the actual three pipeline stages and require success."""

    assert sandbox["bronze_module"].ingest_olist() is True

    bronze_report = json.loads(
        (
            sandbox["output_dir"] / "olist_bronze_manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert bronze_report["success"] is True
    assert bronze_report["tables_succeeded"] == 9
    assert bronze_report["tables_failed"] == 0

    assert sandbox["silver_module"].transform_olist_silver() is True
    silver_report = json.loads(
        (
            sandbox["output_dir"] / "olist_silver_quality.json"
        ).read_text(encoding="utf-8")
    )
    assert silver_report["passed"] is True
    assert all(silver_report["checks"].values())
    assert all(
        result["orphan_rows"] == 0
        for name, result in silver_report["foreign_key_quality"].items()
        if not name.startswith("products.product_category_name->")
    )

    assert sandbox["gold_module"].build_olist_gold() is True
    gold_report = json.loads(
        (
            sandbox["output_dir"] / "olist_gold_quality.json"
        ).read_text(encoding="utf-8")
    )
    assert gold_report["passed"] is True
    assert all(gold_report["checks"].values())
    assert all(
        count == 0
        for count in gold_report["foreign_key_violations"].values()
    )

    return bronze_report, silver_report, gold_report


def load_sorted_gold_frames(gold_dir):
    """Read Gold Parquet output deterministically for before/after comparison."""

    outputs = {}

    for table, sort_keys in GOLD_SORT_KEYS.items():
        frame = pd.read_parquet(gold_dir / f"{table}.parquet")
        outputs[table] = frame.sort_values(
            by=sort_keys
        ).reset_index(drop=True)

    return outputs


def test_synthetic_olist_pipeline_is_reconciled_and_safe_to_rerun(
    monkeypatch, tmp_path
):
    """Run the real Bronze/Silver/Gold flow twice and compare business outputs."""

    sandbox = prepare_sandbox(monkeypatch, tmp_path)
    source_hashes_before = {
        path.name: sha256(path)
        for path in sorted(sandbox["source_dir"].glob("*.csv"))
    }

    bronze_first, silver_first, gold_first = run_bronze_silver_gold(sandbox)

    assert bronze_first["tables_succeeded"] == 9
    assert bronze_first["tables_failed"] == 0
    assert silver_first["row_counts"]["orders"] == 2
    assert silver_first["row_counts"]["order_items"] == 2

    reconciliation = gold_first["reconciliation"]
    assert reconciliation["silver_order_count"] == 2
    assert reconciliation["fact_order_count"] == 2
    assert reconciliation["silver_item_count"] == 2
    assert reconciliation["fact_item_count"] == 2
    assert reconciliation["source_item_revenue"] == 300.0
    assert reconciliation["fact_item_revenue"] == 300.0
    assert reconciliation["daily_product_revenue"] == 300.0
    assert reconciliation["category_product_revenue"] == 300.0
    assert reconciliation["seller_product_revenue"] == 300.0

    first_frames = load_sorted_gold_frames(sandbox["gold_dir"])

    # Re-run the exact same inputs; row counts, monetary aggregates and
    # final Gold tables must remain identical rather than append duplicates.
    bronze_second, silver_second, gold_second = run_bronze_silver_gold(sandbox)

    assert bronze_second["tables_succeeded"] == bronze_first["tables_succeeded"]
    assert silver_second["row_counts"] == silver_first["row_counts"]
    assert gold_second["row_counts"] == gold_first["row_counts"]
    assert gold_second["reconciliation"] == gold_first["reconciliation"]

    second_frames = load_sorted_gold_frames(sandbox["gold_dir"])
    for table in GOLD_SORT_KEYS:
        assert_frame_equal(
            first_frames[table],
            second_frames[table],
            check_dtype=False,
            check_exact=False,
        )

    source_hashes_after = {
        path.name: sha256(path)
        for path in sorted(sandbox["source_dir"].glob("*.csv"))
    }
    assert source_hashes_after == source_hashes_before

    # No temporary publication files should be left behind after success.
    assert not list(sandbox["gold_dir"].glob("*.tmp"))


def test_silver_quality_gate_rejects_duplicate_customer_keys(
    monkeypatch, tmp_path
):
    """A deliberately invalid primary key must stop the Silver stage."""

    sandbox = prepare_sandbox(
        monkeypatch,
        tmp_path,
        duplicate_customer=True,
    )

    assert sandbox["bronze_module"].ingest_olist() is True
    assert sandbox["silver_module"].transform_olist_silver() is False

    silver_report = json.loads(
        (
            sandbox["output_dir"] / "olist_silver_quality.json"
        ).read_text(encoding="utf-8")
    )
    assert silver_report["passed"] is False
    assert silver_report["checks"]["required_keys_unique_and_present"] is False
    assert (
        silver_report["key_quality"]["customers"]["duplicate_key_groups"]
        == 1
    )
