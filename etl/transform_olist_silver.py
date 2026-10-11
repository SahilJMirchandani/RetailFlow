
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "olist"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "olist"
REPORT_FILE = PROJECT_ROOT / "data" / "output" / "olist_silver_quality.json"

TABLE_FILES = {
    "customers": "olist_customers_dataset.parquet",
    "geolocation": "olist_geolocation_dataset.parquet",
    "order_items": "olist_order_items_dataset.parquet",
    "order_payments": "olist_order_payments_dataset.parquet",
    "order_reviews": "olist_order_reviews_dataset.parquet",
    "orders": "olist_orders_dataset.parquet",
    "products": "olist_products_dataset.parquet",
    "sellers": "olist_sellers_dataset.parquet",
    "category_translation": "product_category_name_translation.parquet",
}


def sql_path(path):
    """Return an escaped path for a DuckDB SQL statement."""

    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def scalar(con, query):
    """Return a single integer result from SQL."""

    return int(con.execute(query).fetchone()[0])


def build_silver_views(con, processed_at):
    """Create typed, standardized views from the Bronze tables."""

    transforms = {
        "customers": """
            SELECT
                NULLIF(TRIM(customer_id), '') AS customer_id,
                NULLIF(TRIM(customer_unique_id), '') AS customer_unique_id,
                NULLIF(TRIM(customer_zip_code_prefix), '')
                    AS customer_zip_code_prefix,
                NULLIF(TRIM(customer_city), '') AS customer_city,
                UPPER(NULLIF(TRIM(customer_state), '')) AS customer_state
            FROM raw_customers
        """,

        "geolocation": """
            SELECT
                NULLIF(TRIM(geolocation_zip_code_prefix), '')
                    AS geolocation_zip_code_prefix,
                TRY_CAST(NULLIF(TRIM(geolocation_lat), '') AS DOUBLE)
                    AS geolocation_lat,
                TRY_CAST(NULLIF(TRIM(geolocation_lng), '') AS DOUBLE)
                    AS geolocation_lng,
                NULLIF(TRIM(geolocation_city), '') AS geolocation_city,
                UPPER(NULLIF(TRIM(geolocation_state), ''))
                    AS geolocation_state
            FROM raw_geolocation
        """,

        "order_items": """
            SELECT
                NULLIF(TRIM(order_id), '') AS order_id,
                TRY_CAST(NULLIF(TRIM(order_item_id), '') AS INTEGER)
                    AS order_item_id,
                NULLIF(TRIM(product_id), '') AS product_id,
                NULLIF(TRIM(seller_id), '') AS seller_id,
                TRY_CAST(NULLIF(TRIM(shipping_limit_date), '') AS TIMESTAMP)
                    AS shipping_limit_date,
                TRY_CAST(NULLIF(TRIM(price), '') AS DECIMAL(12, 2))
                    AS price,
                TRY_CAST(NULLIF(TRIM(freight_value), '') AS DECIMAL(12, 2))
                    AS freight_value
            FROM raw_order_items
        """,

        "order_payments": """
            SELECT
                NULLIF(TRIM(order_id), '') AS order_id,
                TRY_CAST(NULLIF(TRIM(payment_sequential), '') AS INTEGER)
                    AS payment_sequential,
                LOWER(NULLIF(TRIM(payment_type), '')) AS payment_type,
                TRY_CAST(NULLIF(TRIM(payment_installments), '') AS INTEGER)
                    AS payment_installments,
                TRY_CAST(NULLIF(TRIM(payment_value), '') AS DECIMAL(12, 2))
                    AS payment_value
            FROM raw_order_payments
        """,

        "order_reviews": """
            SELECT
                NULLIF(TRIM(review_id), '') AS review_id,
                NULLIF(TRIM(order_id), '') AS order_id,
                TRY_CAST(NULLIF(TRIM(review_score), '') AS INTEGER)
                    AS review_score,
                NULLIF(TRIM(review_comment_title), '')
                    AS review_comment_title,
                NULLIF(TRIM(review_comment_message), '')
                    AS review_comment_message,
                TRY_CAST(NULLIF(TRIM(review_creation_date), '') AS TIMESTAMP)
                    AS review_creation_date,
                TRY_CAST(NULLIF(TRIM(review_answer_timestamp), '') AS TIMESTAMP)
                    AS review_answer_timestamp
            FROM raw_order_reviews
        """,

        "orders": """
            SELECT
                NULLIF(TRIM(order_id), '') AS order_id,
                NULLIF(TRIM(customer_id), '') AS customer_id,
                LOWER(NULLIF(TRIM(order_status), '')) AS order_status,
                TRY_CAST(NULLIF(TRIM(order_purchase_timestamp), '') AS TIMESTAMP)
                    AS order_purchase_timestamp,
                TRY_CAST(NULLIF(TRIM(order_approved_at), '') AS TIMESTAMP)
                    AS order_approved_at,
                TRY_CAST(NULLIF(TRIM(order_delivered_carrier_date), '') AS TIMESTAMP)
                    AS order_delivered_carrier_date,
                TRY_CAST(NULLIF(TRIM(order_delivered_customer_date), '') AS TIMESTAMP)
                    AS order_delivered_customer_date,
                TRY_CAST(NULLIF(TRIM(order_estimated_delivery_date), '') AS TIMESTAMP)
                    AS order_estimated_delivery_date
            FROM raw_orders
        """,

        "products": """
            SELECT
                NULLIF(TRIM(product_id), '') AS product_id,
                NULLIF(TRIM(product_category_name), '')
                    AS product_category_name,
                TRY_CAST(NULLIF(TRIM(product_name_lenght), '') AS INTEGER)
                    AS product_name_length,
                TRY_CAST(NULLIF(TRIM(product_description_lenght), '') AS INTEGER)
                    AS product_description_length,
                TRY_CAST(NULLIF(TRIM(product_photos_qty), '') AS INTEGER)
                    AS product_photos_qty,
                TRY_CAST(NULLIF(TRIM(product_weight_g), '') AS DOUBLE)
                    AS product_weight_g,
                TRY_CAST(NULLIF(TRIM(product_length_cm), '') AS DOUBLE)
                    AS product_length_cm,
                TRY_CAST(NULLIF(TRIM(product_height_cm), '') AS DOUBLE)
                    AS product_height_cm,
                TRY_CAST(NULLIF(TRIM(product_width_cm), '') AS DOUBLE)
                    AS product_width_cm
            FROM raw_products
        """,

        "sellers": """
            SELECT
                NULLIF(TRIM(seller_id), '') AS seller_id,
                NULLIF(TRIM(seller_zip_code_prefix), '')
                    AS seller_zip_code_prefix,
                NULLIF(TRIM(seller_city), '') AS seller_city,
                UPPER(NULLIF(TRIM(seller_state), '')) AS seller_state
            FROM raw_sellers
        """,

        "category_translation": """
            SELECT
                NULLIF(TRIM(product_category_name), '')
                    AS product_category_name,
                NULLIF(TRIM(product_category_name_english), '')
                    AS product_category_name_english
            FROM raw_category_translation
        """,
    }

    for name, filename in TABLE_FILES.items():
        source = BRONZE_DIR / filename

        if not source.is_file():
            raise FileNotFoundError(
                f"Missing Bronze table: {source}"
            )

        con.execute(
            f"CREATE OR REPLACE TEMP VIEW raw_{name} AS "
            f"SELECT * FROM read_parquet('{sql_path(source)}')"
        )

    for name, query in transforms.items():
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW silver_{name} AS
            SELECT
                normalized.*,
                'olist' AS source_system,
                '{processed_at}' AS silver_processed_at_utc
            FROM ({query}) AS normalized
        """)


def check_key_quality(con, table, keys):
    """Check missing keys and duplicate key groups."""

    key_condition = " OR ".join(
        f'"{key}" IS NULL' for key in keys
    )

    key_columns = ", ".join(
        f'"{key}"' for key in keys
    )

    null_rows = scalar(
        con,
        f"""
            SELECT COUNT(*)
            FROM silver_{table}
            WHERE {key_condition}
        """,
    )

    duplicate_groups = scalar(
        con,
        f"""
            SELECT COUNT(*)
            FROM (
                SELECT {key_columns}
                FROM silver_{table}
                WHERE NOT ({key_condition})
                GROUP BY {key_columns}
                HAVING COUNT(*) > 1
            ) duplicates
        """,
    )

    return {
        "required_keys": keys,
        "rows_with_missing_keys": null_rows,
        "duplicate_key_groups": duplicate_groups,
        "passed": null_rows == 0 and duplicate_groups == 0,
    }


def check_foreign_key(con, child, child_key, parent, parent_key):
    """Count child records whose non-null foreign key has no parent."""

    count = scalar(
        con,
        f"""
            SELECT COUNT(*)
            FROM silver_{child} child
            WHERE child."{child_key}" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM silver_{parent} parent
                  WHERE parent."{parent_key}" = child."{child_key}"
              )
        """,
    )

    return {
        "child_table": child,
        "child_column": child_key,
        "parent_table": parent,
        "parent_column": parent_key,
        "orphan_rows": count,
        "passed": count == 0,
    }


def transform_olist_silver():
    """Build typed Silver tables and generate a quality report."""

    print("=" * 70)
    print("RetailFlow 2.0 - Olist Silver Transformation")
    print("=" * 70)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

    processed_at = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(database=":memory:")

    try:
        build_silver_views(con, processed_at)

        row_counts = {}
        output_files = {}

        print("\nWriting Silver Parquet tables...")

        for name in TABLE_FILES:
            view = f"silver_{name}"
            output = SILVER_DIR / f"{name}.parquet"

            output.unlink(missing_ok=True)

            con.execute(
                f"COPY (SELECT * FROM {view}) "
                f"TO '{sql_path(output)}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD)"
            )

            row_counts[name] = scalar(
                con,
                f"SELECT COUNT(*) FROM {view}",
            )

            output_files[name] = {
                "path": output.relative_to(
                    PROJECT_ROOT
                ).as_posix(),
                "size_bytes": output.stat().st_size,
            }

            print(
                f"  {name:<22} {row_counts[name]:>10,} rows"
            )

        # Primary and composite key checks.
        key_definitions = {
            "customers": ["customer_id"],
            "orders": ["order_id"],
            "products": ["product_id"],
            "sellers": ["seller_id"],
            "order_items": ["order_id", "order_item_id"],
            "order_payments": ["order_id", "payment_sequential"],
            "order_reviews": ["review_id", "order_id"],
            "category_translation": ["product_category_name"],
        }

        key_results = {
            name: check_key_quality(con, name, keys)
            for name, keys in key_definitions.items()
        }

        # Referential integrity checks on legitimate relationships.
        foreign_keys = [
            ("orders", "customer_id", "customers", "customer_id"),
            ("order_items", "order_id", "orders", "order_id"),
            ("order_items", "product_id", "products", "product_id"),
            ("order_items", "seller_id", "sellers", "seller_id"),
            ("order_payments", "order_id", "orders", "order_id"),
            ("order_reviews", "order_id", "orders", "order_id"),
            (
                "products",
                "product_category_name",
                "category_translation",
                "product_category_name",
            ),
        ]

        foreign_key_results = {}

        for child, child_key, parent, parent_key in foreign_keys:
            name = f"{child}.{child_key}->{parent}.{parent_key}"

            foreign_key_results[name] = check_foreign_key(
                con, child, child_key, parent, parent_key
            )

        # Product-category translation gaps are informational.
        # A category absent from the translation table is not necessarily
        # an invalid product, so it is not treated as an orphan failure.
        unmapped_categories = scalar(
            con,
            """
                SELECT COUNT(DISTINCT p.product_category_name)
                FROM silver_products p
                WHERE p.product_category_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM silver_category_translation t
                      WHERE t.product_category_name =
                            p.product_category_name
                  )
            """,
        )

        # Validate required measures and sensible ranges.
        invalid_item_amounts = scalar(
            con,
            """
                SELECT COUNT(*)
                FROM silver_order_items
                WHERE price IS NULL
                   OR freight_value IS NULL
                   OR price < 0
                   OR freight_value < 0
            """,
        )

        invalid_payment_amounts = scalar(
            con,
            """
                SELECT COUNT(*)
                FROM silver_order_payments
                WHERE payment_value IS NULL
                   OR payment_value < 0
            """,
        )

        invalid_review_scores = scalar(
            con,
            """
                SELECT COUNT(*)
                FROM silver_order_reviews
                WHERE review_score IS NOT NULL
                  AND review_score NOT BETWEEN 1 AND 5
            """,
        )

        purchase_timestamps_missing = scalar(
            con,
            """
                SELECT COUNT(*)
                FROM silver_orders
                WHERE order_purchase_timestamp IS NULL
            """,
        )

        nonempty_tables = all(
            count > 0 for count in row_counts.values()
        )

        keys_passed = all(
            result["passed"] for result in key_results.values()
        )

        # Exclude category translations from the blocking integrity
        # rule because untranslated product categories are expected
        # in real multi-source data.
        critical_fk_results = {
            name: result
            for name, result in foreign_key_results.items()
            if not name.startswith("products.product_category_name->")
        }

        foreign_keys_passed = all(
            result["passed"]
            for result in critical_fk_results.values()
        )

        checks = {
            "all_tables_nonempty": nonempty_tables,
            "required_keys_unique_and_present": keys_passed,
            "core_referential_integrity": foreign_keys_passed,
            "valid_order_item_amounts": invalid_item_amounts == 0,
            "valid_payment_amounts": invalid_payment_amounts == 0,
            "valid_review_score_ranges": invalid_review_scores == 0,
            "order_purchase_timestamps_present":
                purchase_timestamps_missing == 0,
        }

        report = {
            "project": "RetailFlow",
            "dataset": "Olist Brazilian E-Commerce",
            "processed_at_utc": processed_at,
            "bronze_directory": BRONZE_DIR.relative_to(
                PROJECT_ROOT
            ).as_posix(),
            "silver_directory": SILVER_DIR.relative_to(
                PROJECT_ROOT
            ).as_posix(),
            "row_counts": row_counts,
            "output_files": output_files,
            "key_quality": key_results,
            "foreign_key_quality": foreign_key_results,
            "informational": {
                "distinct_unmapped_product_categories":
                    unmapped_categories,
                "invalid_order_item_amount_rows":
                    invalid_item_amounts,
                "invalid_payment_amount_rows":
                    invalid_payment_amounts,
                "invalid_review_score_rows":
                    invalid_review_scores,
                "missing_purchase_timestamps":
                    purchase_timestamps_missing,
            },
            "checks": checks,
            "passed": all(checks.values()),
        }

        REPORT_FILE.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        print("\nDATA QUALITY SUMMARY")
        print("-" * 50)

        for name, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}  {name}")

        print(
            "\nDistinct product categories without translations:",
            unmapped_categories,
        )

        print(f"\nQuality report: {REPORT_FILE}")

        if not report["passed"]:
            print(
                "\nSilver transformation completed, but one or more "
                "quality checks failed."
            )
            print(
                "Review olist_silver_quality.json before using "
                "these outputs downstream."
            )
            return False

        print("\nAll Silver quality checks passed.")
        return True

    finally:
        con.close()


if __name__ == "__main__":
    try:
        success = transform_olist_silver()
    except Exception as error:
        print(f"\nSILVER TRANSFORMATION ERROR: {error}")
        success = False

    if not success:
        sys.exit(1)
