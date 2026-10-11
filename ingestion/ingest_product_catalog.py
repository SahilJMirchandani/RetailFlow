import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

API_URL = "https://dummyjson.com/products"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 30

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "dummyjson"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "products"
REPORT_FILE = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "product_catalog_quality.json"
)

NUMERIC_COLUMNS = [
    "price",
    "discount_percentage",
    "rating",
    "stock",
    "minimum_order_quantity",
]

REQUIRED_COLUMNS = [
    "source_system",
    "source_product_id",
    "title",
    "category",
    "price",
    "thumbnail_url",
    "image_urls_json",
    "ingested_at_utc",
]


# --------------------------------------------------
# HTTP SESSION WITH RETRIES
# --------------------------------------------------

def create_session():
    """Create a session with retries for temporary API errors."""

    retry_config = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_config)

    session = requests.Session()
    session.mount("https://", adapter)

    return session


# --------------------------------------------------
# BRONZE: FETCH AND PRESERVE RAW API RESPONSES
# --------------------------------------------------

def fetch_products():
    """Fetch every product using API pagination."""

    all_products = []
    raw_pages = []
    skip = 0
    reported_total = None

    with create_session() as session:
        while True:
            params = {
                "limit": PAGE_SIZE,
                "skip": skip,
            }

            response = session.get(
                API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("API returned an invalid response.")

            batch = payload.get("products")

            if not isinstance(batch, list):
                raise ValueError(
                    "API response does not contain a product list."
                )

            reported_total = int(
                payload.get("total", len(all_products) + len(batch))
            )

            # Preserve the response page without transforming it.
            raw_pages.append({
                "request_params": params,
                "response": payload,
            })

            all_products.extend(batch)

            print(
                f"Fetched {len(batch)} products; "
                f"total collected: {len(all_products)}"
            )

            if not batch or len(all_products) >= reported_total:
                break

            skip += len(batch)

    if not all_products:
        raise ValueError("The API returned no products.")

    if len(all_products) != reported_total:
        raise ValueError(
            f"Incomplete ingestion: expected {reported_total} products, "
            f"received {len(all_products)}."
        )

    return all_products, raw_pages, reported_total


# --------------------------------------------------
# SILVER: NORMALIZE PRODUCT RECORDS
# --------------------------------------------------

def normalize_products(products, ingested_at):
    """Convert API records into a consistent tabular schema."""

    rows = []

    for product in products:
        metadata = product.get("meta") or {}
        images = product.get("images") or []

        if not isinstance(images, list):
            images = []

        # Retain valid image URL strings.
        images = [
            url.strip()
            for url in images
            if isinstance(url, str) and url.strip()
        ]

        thumbnail = product.get("thumbnail")

        if not thumbnail and images:
            thumbnail = images[0]

        rows.append({
            "source_system": "dummyjson",
            "source_product_id": (
                str(product["id"])
                if product.get("id") is not None
                else None
            ),
            "title": product.get("title"),
            "description": product.get("description"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "price": product.get("price"),
            "discount_percentage": product.get(
                "discountPercentage"
            ),
            "rating": product.get("rating"),
            "stock": product.get("stock"),
            "sku": product.get("sku"),
            "availability_status": product.get("availabilityStatus"),
            "minimum_order_quantity": product.get(
                "minimumOrderQuantity"
            ),
            "thumbnail_url": thumbnail,
            "image_urls_json": json.dumps(images),
            "source_updated_at": metadata.get("updatedAt"),
            "ingested_at_utc": ingested_at,
        })

    df = pd.DataFrame(rows)

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    return df


# --------------------------------------------------
# DATA QUALITY REPORT
# --------------------------------------------------

def validate_catalog(df, reported_total):
    """Check schema, identifiers, prices and image coverage."""

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    duplicate_ids = int(
        df["source_product_id"].duplicated().sum()
    )

    missing_ids = int(
        df["source_product_id"].isna().sum()
    )

    missing_titles = int(
        df["title"].isna().sum()
        + df["title"].astype("string").str.strip().eq("").sum()
    )

    invalid_prices = int(
        df["price"].isna().sum()
        + (df["price"] <= 0).sum()
    )

    # An image is considered present when a thumbnail URL exists.
    products_with_images = int(
        df["thumbnail_url"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
        .sum()
    )

    total_image_urls = sum(
        len(json.loads(value))
        for value in df["image_urls_json"]
    )

    checks = {
        "non_empty_dataset": len(df) > 0,
        "required_schema_present": not missing_columns,
        "record_count_matches_api": len(df) == reported_total,
        "unique_product_ids": duplicate_ids == 0,
        "product_ids_present": missing_ids == 0,
        "product_titles_present": missing_titles == 0,
        "valid_positive_prices": invalid_prices == 0,
    }

    report = {
        "source": "DummyJSON Products API",
        "validated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "records_received": len(df),
        "records_reported_by_api": reported_total,
        "missing_columns": missing_columns,
        "duplicate_product_ids": duplicate_ids,
        "missing_product_ids": missing_ids,
        "missing_titles": missing_titles,
        "invalid_prices": invalid_prices,
        "products_with_thumbnail": products_with_images,
        "total_product_image_urls": total_image_urls,
        "distinct_categories": int(df["category"].nunique()),
        "checks": checks,
        "passed": all(checks.values()),
    }

    return report


# --------------------------------------------------
# MAIN INGESTION WORKFLOW
# --------------------------------------------------

def ingest_product_catalog():
    """Run the product API ingestion and Silver transformation."""

    print("=" * 65)
    print("RetailFlow 2.0 - Product Catalogue Ingestion")
    print("=" * 65)

    ingested_at = datetime.now(timezone.utc).isoformat()

    try:
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Fetch data.
        products, raw_pages, reported_total = fetch_products()

        # Preserve the original API responses in Bronze.
        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )

        bronze_file = (
            BRONZE_DIR / f"products_{timestamp}.json"
        )

        raw_snapshot = {
            "source": API_URL,
            "ingested_at_utc": ingested_at,
            "reported_total": reported_total,
            "records_received": len(products),
            "pages": raw_pages,
        }

        bronze_file.write_text(
            json.dumps(raw_snapshot, indent=2),
            encoding="utf-8",
        )

        # Standardize the data into a table.
        df = normalize_products(products, ingested_at)

        # Generate and persist a quality report.
        report = validate_catalog(df, reported_total)

        REPORT_FILE.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        print("\nDATA QUALITY RESULTS")
        print("-" * 40)
        print(json.dumps(report, indent=2))

        if not report["passed"]:
            print("\nIngestion stopped: data quality checks failed.")
            return False

        # Store repeatable latest Silver outputs.
        parquet_file = SILVER_DIR / "products.parquet"
        csv_file = SILVER_DIR / "products.csv"

        df.to_parquet(parquet_file, index=False)
        df.to_csv(csv_file, index=False)

        print("\nINGESTION COMPLETED SUCCESSFULLY")
        print("-" * 40)
        print(f"Products received : {len(df)}")
        print(f"Bronze JSON       : {bronze_file}")
        print(f"Silver Parquet    : {parquet_file}")
        print(f"Silver CSV        : {csv_file}")
        print(f"Quality report    : {REPORT_FILE}")
        print("Images retained as source URLs; files not downloaded yet.")

        return True

    except (requests.RequestException, ValueError, OSError) as error:
        print(f"\nINGESTION ERROR: {error}")
        return False

    except Exception as error:
        print(f"\nUNEXPECTED ERROR: {error}")
        return False


if __name__ == "__main__":
    success = ingest_product_catalog()

    if not success:
        sys.exit(1)