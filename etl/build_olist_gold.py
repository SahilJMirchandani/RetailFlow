
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "olist"
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "olist"

REPORT_FILE = (
    PROJECT_ROOT / "data" / "output" / "olist_gold_quality.json"
)

SILVER_TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "category_translation",
]


def sql_path(path):
    """Escape a local path for DuckDB SQL."""
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def count_rows(con, view):
    """Return the number of rows in a SQL view."""
    return int(
        con.execute(
            f"SELECT COUNT(*) FROM {view}"
        ).fetchone()[0]
    )


def number_value(con, query):
    """Return a numeric SQL result, treating NULL as zero."""
    value = con.execute(query).fetchone()[0]
    return float(value or 0)


def create_silver_views(con):
    """Register all Silver Parquet tables with DuckDB."""

    for table in SILVER_TABLES:
        file_path = SILVER_DIR / f"{table}.parquet"

        if not file_path.is_file():
            raise FileNotFoundError(
                f"Missing Silver table: {file_path}"
            )

        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW s_{table} AS
            SELECT *
            FROM read_parquet('{sql_path(file_path)}')
            """
        )


def create_gold_views(con):
    """Create dimensions, facts and business aggregates."""

    # --------------------------------------------------
    # DIMENSION TABLES
    # --------------------------------------------------

    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_customers AS
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM s_customers
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_products AS
        SELECT
            p.product_id,
            p.product_category_name,
            COALESCE(
                t.product_category_name_english,
                p.product_category_name,
                'Uncategorized'
            ) AS product_category,
            t.product_category_name_english
                AS product_category_english,
            p.product_name_length,
            p.product_description_length,
            p.product_photos_qty,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm
        FROM s_products p
        LEFT JOIN s_category_translation t
          ON p.product_category_name =
             t.product_category_name
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_sellers AS
        SELECT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM s_sellers
    """)

    # A continuous date dimension covers the entire
    # range of purchase dates, including days with no orders.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_date AS
        SELECT
            CAST(calendar_date AS DATE) AS calendar_date,
            CAST(
                STRFTIME(
                    CAST(calendar_date AS DATE),
                    '%Y%m%d'
                ) AS INTEGER
            ) AS date_key,
            YEAR(CAST(calendar_date AS DATE))
                AS calendar_year,
            QUARTER(CAST(calendar_date AS DATE))
                AS calendar_quarter,
            MONTH(CAST(calendar_date AS DATE))
                AS calendar_month,
            STRFTIME(
                CAST(calendar_date AS DATE), '%B'
            ) AS month_name,
            DAY(CAST(calendar_date AS DATE))
                AS day_of_month,
            STRFTIME(
                CAST(calendar_date AS DATE), '%A'
            ) AS day_name,
            CAST(
                STRFTIME(
                    CAST(calendar_date AS DATE), '%w'
                ) AS INTEGER
            ) AS day_of_week_number,
            STRFTIME(
                CAST(calendar_date AS DATE), '%w'
            ) IN ('0', '6') AS is_weekend
        FROM GENERATE_SERIES(
            (
                SELECT MIN(
                    CAST(order_purchase_timestamp AS DATE)
                )
                FROM s_orders
            ),
            (
                SELECT MAX(
                    CAST(order_purchase_timestamp AS DATE)
                )
                FROM s_orders
            ),
            INTERVAL 1 DAY
        ) AS dates(calendar_date)
    """)

    # --------------------------------------------------
    # FACT: ORDER ITEMS
    # Grain: one row per order_id + order_item_id.
    # --------------------------------------------------

    con.execute("""
        CREATE OR REPLACE TEMP VIEW fact_order_items AS
        SELECT
            oi.order_id,
            oi.order_item_id,
            o.customer_id,
            oi.product_id,
            oi.seller_id,

            CAST(
                o.order_purchase_timestamp AS DATE
            ) AS purchase_date,

            CAST(
                STRFTIME(
                    CAST(o.order_purchase_timestamp AS DATE),
                    '%Y%m%d'
                ) AS INTEGER
            ) AS purchase_date_key,

            o.order_status,
            oi.price AS item_price,
            oi.freight_value,

            CAST(
                oi.price + oi.freight_value
                AS DECIMAL(14, 2)
            ) AS item_total_including_freight

        FROM s_order_items oi
        INNER JOIN s_orders o
          ON oi.order_id = o.order_id
    """)

    # --------------------------------------------------
    # FACT: ORDERS
    # Grain: one row per order_id.
    #
    # Payments, reviews and items are aggregated
    # separately before joining to prevent row fan-out.
    # --------------------------------------------------

    con.execute("""
        CREATE OR REPLACE TEMP VIEW fact_orders AS
        WITH item_metrics AS (
            SELECT
                order_id,
                COUNT(*) AS item_count,
                SUM(price) AS product_revenue,
                SUM(freight_value) AS freight_total
            FROM s_order_items
            GROUP BY order_id
        ),

        payment_metrics AS (
            SELECT
                order_id,
                COUNT(*) AS payment_record_count,
                SUM(payment_value) AS payment_total
            FROM s_order_payments
            GROUP BY order_id
        ),

        review_metrics AS (
            SELECT
                order_id,
                COUNT(*) AS review_record_count,
                AVG(review_score) AS average_review_score
            FROM s_order_reviews
            GROUP BY order_id
        )

        SELECT
            o.order_id,
            o.customer_id,
            o.order_status,

            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,

            CAST(
                o.order_purchase_timestamp AS DATE
            ) AS purchase_date,

            CAST(
                STRFTIME(
                    CAST(o.order_purchase_timestamp AS DATE),
                    '%Y%m%d'
                ) AS INTEGER
            ) AS purchase_date_key,

            CAST(
                COALESCE(i.item_count, 0) AS BIGINT
            ) AS item_count,

            CAST(
                COALESCE(i.product_revenue, 0)
                AS DECIMAL(14, 2)
            ) AS product_revenue,

            CAST(
                COALESCE(i.freight_total, 0)
                AS DECIMAL(14, 2)
            ) AS freight_total,

            CAST(
                COALESCE(p.payment_total, 0)
                AS DECIMAL(14, 2)
            ) AS payment_total,

            CAST(
                COALESCE(p.payment_record_count, 0)
                AS BIGINT
            ) AS payment_record_count,

            CAST(
                COALESCE(r.review_record_count, 0)
                AS BIGINT
            ) AS review_record_count,

            CAST(
                r.average_review_score AS DOUBLE
            ) AS average_review_score

        FROM s_orders o

        LEFT JOIN item_metrics i
          ON o.order_id = i.order_id

        LEFT JOIN payment_metrics p
          ON o.order_id = p.order_id

        LEFT JOIN review_metrics r
          ON o.order_id = r.order_id
    """)

    # --------------------------------------------------
    # GOLD BUSINESS AGGREGATES
    # --------------------------------------------------

    con.execute("""
        CREATE OR REPLACE TEMP VIEW gold_daily_sales AS
        SELECT
            purchase_date AS order_date,
            COUNT(*) AS order_count,
            SUM(item_count) AS item_count,
            SUM(product_revenue) AS product_revenue,
            SUM(freight_total) AS freight_total,
            SUM(payment_total) AS payment_total,
            SUM(review_record_count) AS review_record_count,
            AVG(average_review_score) AS average_review_score
        FROM fact_orders
        GROUP BY purchase_date
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW gold_category_sales AS
        SELECT
            p.product_category,
            COUNT(*) AS item_count,
            COUNT(DISTINCT f.order_id) AS order_count,
            SUM(f.item_price) AS product_revenue,
            SUM(f.freight_value) AS freight_total
        FROM fact_order_items f
        INNER JOIN dim_products p
          ON f.product_id = p.product_id
        GROUP BY p.product_category
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW gold_seller_performance AS
        SELECT
            s.seller_id,
            s.seller_city,
            s.seller_state,
            COUNT(*) AS item_count,
            COUNT(DISTINCT f.order_id) AS order_count,
            SUM(f.item_price) AS product_revenue,
            SUM(f.freight_value) AS freight_total
        FROM fact_order_items f
        INNER JOIN dim_sellers s
          ON f.seller_id = s.seller_id
        GROUP BY
            s.seller_id,
            s.seller_city,
            s.seller_state
    """)


def duplicate_groups(con, view, columns):
    """Count duplicate groups for a candidate key."""

    key_sql = ", ".join(columns)

    return int(con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT {key_sql}
            FROM {view}
            GROUP BY {key_sql}
            HAVING COUNT(*) > 1
        ) duplicates
    """).fetchone()[0])


def build_olist_gold():
    """Build and validate the Olist Gold warehouse."""

    print("=" * 70)
    print("RetailFlow 2.0 - Gold Analytical Warehouse")
    print("=" * 70)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    processed_at = datetime.now(timezone.utc).isoformat()

    try:
        create_silver_views(con)
        create_gold_views(con)

        gold_views = [
            "dim_customers",
            "dim_products",
            "dim_sellers",
            "dim_date",
            "fact_orders",
            "fact_order_items",
            "gold_daily_sales",
            "gold_category_sales",
            "gold_seller_performance",
        ]

        row_counts = {}
        output_files = {}

        print("\nWriting Gold Parquet tables...")

        for view in gold_views:
            parquet_file = GOLD_DIR / f"{view}.parquet"

            con.execute(
                f"""
                COPY (SELECT * FROM {view})
                TO '{sql_path(parquet_file)}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )

            row_counts[view] = count_rows(con, view)

            output_files[view] = {
                "path": parquet_file.relative_to(
                    PROJECT_ROOT
                ).as_posix(),
                "size_bytes": parquet_file.stat().st_size,
            }

            # Export the smaller business aggregates as CSV as well.
            if view.startswith("gold_"):
                csv_file = GOLD_DIR / f"{view}.csv"

                con.execute(
                    f"""
                    COPY (SELECT * FROM {view})
                    TO '{sql_path(csv_file)}'
                    (HEADER, DELIMITER ',')
                    """
                )

                output_files[f"{view}_csv"] = {
                    "path": csv_file.relative_to(
                        PROJECT_ROOT
                    ).as_posix(),
                    "size_bytes": csv_file.stat().st_size,
                }

            print(
                f"  {view:<28} {row_counts[view]:>10,} rows"
            )

        # --------------------------------------------------
        # QUALITY AND RECONCILIATION CHECKS
        # --------------------------------------------------

        duplicate_key_counts = {
            "dim_customers.customer_id": duplicate_groups(
                con, "dim_customers", ["customer_id"]
            ),
            "dim_products.product_id": duplicate_groups(
                con, "dim_products", ["product_id"]
            ),
            "dim_sellers.seller_id": duplicate_groups(
                con, "dim_sellers", ["seller_id"]
            ),
            "dim_date.date_key": duplicate_groups(
                con, "dim_date", ["date_key"]
            ),
            "fact_orders.order_id": duplicate_groups(
                con, "fact_orders", ["order_id"]
            ),
            "fact_order_items.order_id_order_item_id":
                duplicate_groups(
                    con,
                    "fact_order_items",
                    ["order_id", "order_item_id"],
                ),
        }

        invalid_dimension_keys = int(con.execute("""
            SELECT
                (SELECT COUNT(*) FROM dim_customers
                 WHERE customer_id IS NULL)
                +
                (SELECT COUNT(*) FROM dim_products
                 WHERE product_id IS NULL)
                +
                (SELECT COUNT(*) FROM dim_sellers
                 WHERE seller_id IS NULL)
                +
                (SELECT COUNT(*) FROM dim_date
                 WHERE date_key IS NULL)
        """).fetchone()[0])

        foreign_key_violations = {
            "fact_orders.customer_id": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_orders f
                LEFT JOIN dim_customers d
                  ON f.customer_id = d.customer_id
                WHERE d.customer_id IS NULL
            """).fetchone()[0]),
            "fact_orders.purchase_date_key": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_orders f
                LEFT JOIN dim_date d
                  ON f.purchase_date_key = d.date_key
                WHERE d.date_key IS NULL
            """).fetchone()[0]),
            "fact_order_items.order_id": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_order_items f
                LEFT JOIN fact_orders o
                  ON f.order_id = o.order_id
                WHERE o.order_id IS NULL
            """).fetchone()[0]),
            "fact_order_items.customer_id": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_order_items f
                LEFT JOIN dim_customers d
                  ON f.customer_id = d.customer_id
                WHERE d.customer_id IS NULL
            """).fetchone()[0]),
            "fact_order_items.product_id": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_order_items f
                LEFT JOIN dim_products d
                  ON f.product_id = d.product_id
                WHERE d.product_id IS NULL
            """).fetchone()[0]),
            "fact_order_items.seller_id": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_order_items f
                LEFT JOIN dim_sellers d
                  ON f.seller_id = d.seller_id
                WHERE d.seller_id IS NULL
            """).fetchone()[0]),
            "fact_order_items.purchase_date_key": int(con.execute("""
                SELECT COUNT(*)
                FROM fact_order_items f
                LEFT JOIN dim_date d
                  ON f.purchase_date_key = d.date_key
                WHERE d.date_key IS NULL
            """).fetchone()[0]),
        }

        silver_item_count = count_rows(con, "s_order_items")
        silver_order_count = count_rows(con, "s_orders")

        fact_item_count = row_counts["fact_order_items"]
        fact_order_count = row_counts["fact_orders"]

        daily_order_count = int(
            con.execute(
                "SELECT COALESCE(SUM(order_count), 0) "
                "FROM gold_daily_sales"
            ).fetchone()[0]
        )

        source_item_revenue = number_value(
            con,
            "SELECT SUM(price) FROM s_order_items",
        )

        fact_item_revenue = number_value(
            con,
            "SELECT SUM(item_price) FROM fact_order_items",
        )

        daily_product_revenue = number_value(
            con,
            "SELECT SUM(product_revenue) FROM gold_daily_sales",
        )

        category_product_revenue = number_value(
            con,
            "SELECT SUM(product_revenue) FROM gold_category_sales",
        )

        seller_product_revenue = number_value(
            con,
            "SELECT SUM(product_revenue) FROM gold_seller_performance",
        )

        revenue_tolerance = 0.01

        checks = {
            "all_gold_tables_nonempty": all(
                count > 0 for count in row_counts.values()
            ),
            "dimension_keys_present": invalid_dimension_keys == 0,
            "all_declared_primary_keys_unique": all(
                count == 0
                for count in duplicate_key_counts.values()
            ),
            "all_fact_foreign_keys_resolve": all(
                count == 0
                for count in foreign_key_violations.values()
            ),
            "fact_item_count_matches_silver":
                fact_item_count == silver_item_count,
            "fact_order_count_matches_silver":
                fact_order_count == silver_order_count,
            "daily_order_count_reconciles":
                daily_order_count == silver_order_count,
            "fact_item_revenue_reconciles": abs(
                fact_item_revenue - source_item_revenue
            ) < revenue_tolerance,
            "daily_revenue_reconciles": abs(
                daily_product_revenue - source_item_revenue
            ) < revenue_tolerance,
            "category_revenue_reconciles": abs(
                category_product_revenue - source_item_revenue
            ) < revenue_tolerance,
            "seller_revenue_reconciles": abs(
                seller_product_revenue - source_item_revenue
            ) < revenue_tolerance,
        }

        report = {
            "project": "RetailFlow",
            "dataset": "Olist Brazilian E-Commerce",
            "processed_at_utc": processed_at,
            "gold_directory": GOLD_DIR.relative_to(
                PROJECT_ROOT
            ).as_posix(),
            "row_counts": row_counts,
            "output_files": output_files,
            "duplicate_key_groups": duplicate_key_counts,
            "invalid_dimension_keys": invalid_dimension_keys,
            "foreign_key_violations": foreign_key_violations,
            "reconciliation": {
                "silver_order_count": silver_order_count,
                "fact_order_count": fact_order_count,
                "daily_order_count": daily_order_count,
                "silver_item_count": silver_item_count,
                "fact_item_count": fact_item_count,
                "source_item_revenue": round(
                    source_item_revenue, 2
                ),
                "fact_item_revenue": round(
                    fact_item_revenue, 2
                ),
                "daily_product_revenue": round(
                    daily_product_revenue, 2
                ),
                "category_product_revenue": round(
                    category_product_revenue, 2
                ),
                "seller_product_revenue": round(
                    seller_product_revenue, 2
                ),
            },
            "checks": checks,
            "passed": all(checks.values()),
        }

        REPORT_FILE.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        print("\nGOLD QUALITY CHECKS")
        print("-" * 50)

        for name, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}  {name}")

        print(f"\nQuality report: {REPORT_FILE}")

        if not report["passed"]:
            print(
                "\nGold outputs were generated, but validation failed. "
                "Review the report before using them downstream."
            )
            return False

        print("\nGold warehouse built and reconciled successfully.")
        return True

    finally:
        con.close()


if __name__ == "__main__":
    try:
        success = build_olist_gold()
    except Exception as error:
        print(f"\nGOLD BUILD ERROR: {error}")
        success = False

    if not success:
        sys.exit(1)
