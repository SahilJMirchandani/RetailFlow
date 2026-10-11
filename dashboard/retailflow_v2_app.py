
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.currency import (
    USD_TO_INR_RATE,
    USD_TO_INR_RATE_AS_OF,
    USD_TO_INR_SOURCE_URL,
    convert_usd_to_inr,
)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

st.set_page_config(
    page_title="RetailFlow 2.0",
    page_icon="🛍️",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }
    [data-testid="stMetric"] {
        border: 1px solid rgba(128,128,128,0.25);
        border-radius: 12px;
        padding: 14px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------
# DATA LOADERS
# --------------------------------------------------

@st.cache_data(show_spinner=False)
def load_parquet(relative_path):
    path = PROJECT_ROOT / relative_path

    if not path.is_file():
        return pd.DataFrame()

    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_csv(relative_path):
    path = PROJECT_ROOT / relative_path

    if not path.is_file():
        return pd.DataFrame()

    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_json(relative_path):
    path = PROJECT_ROOT / relative_path

    if not path.is_file():
        return {}

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}


@st.cache_data(show_spinner=False)
def load_catalog():
    products = load_parquet(
        "data/silver/products/products.parquet"
    )

    manifest = load_csv(
        "data/output/product_images_manifest.csv"
    )

    if not manifest.empty:
        manifest["source_product_id"] = (
            manifest["source_product_id"].astype(str)
        )

        manifest["is_thumbnail"] = (
            manifest["is_thumbnail"]
            .astype(str)
            .str.lower()
            .isin(["true", "1", "yes"])
        )

    if not products.empty:
        products["source_product_id"] = (
            products["source_product_id"].astype(str)
        )

    return products, manifest


def format_currency(value, currency="R$"):
    return f"{currency} {float(value or 0):,.2f}"


def get_thumbnail_map(manifest):
    """Map each product ID to its locally downloaded thumbnail."""

    if manifest.empty:
        return {}

    thumbnails = manifest[
        manifest["is_thumbnail"]
        & manifest["local_path"].notna()
    ].drop_duplicates("source_product_id")

    return dict(
        zip(
            thumbnails["source_product_id"],
            thumbnails["local_path"],
        )
    )


def show_local_image(local_path, fallback_url=None):
    """Display a local image, using its URL only as a fallback."""

    if isinstance(local_path, str) and local_path:
        path = PROJECT_ROOT / Path(local_path)

        if path.is_file():
            st.image(str(path), use_container_width=True)
            return

    if isinstance(fallback_url, str) and fallback_url.startswith("https://"):
        st.image(fallback_url, use_container_width=True)
    else:
        st.info("Image unavailable")


# --------------------------------------------------
# LOAD CURRENT PIPELINE OUTPUTS
# --------------------------------------------------

products, image_manifest = load_catalog()

fact_orders = load_parquet(
    "data/gold/olist/fact_orders.parquet"
)

fact_items = load_parquet(
    "data/gold/olist/fact_order_items.parquet"
)

customers = load_parquet(
    "data/gold/olist/dim_customers.parquet"
)

gold_products = load_parquet(
    "data/gold/olist/dim_products.parquet"
)

daily_sales = load_parquet(
    "data/gold/olist/gold_daily_sales.parquet"
)

category_sales = load_parquet(
    "data/gold/olist/gold_category_sales.parquet"
)

seller_sales = load_parquet(
    "data/gold/olist/gold_seller_performance.parquet"
)

payments = load_parquet(
    "data/silver/olist/order_payments.parquet"
)

pipeline_run = load_json(
    "data/output/lineage/pipeline_run_latest.json"
)

bronze_report = load_json(
    "data/output/olist_bronze_manifest.json"
)

silver_report = load_json(
    "data/output/olist_silver_quality.json"
)

gold_report = load_json(
    "data/output/olist_gold_quality.json"
)

catalog_report = load_json(
    "data/output/product_catalog_quality.json"
)

thumbnail_map = get_thumbnail_map(image_manifest)

# --------------------------------------------------
# NORMALIZE PANDAS DATA TYPES
# --------------------------------------------------

# Convert numeric columns to numeric types.
# Parquet DECIMAL columns may be loaded as object dtype.
numeric_columns = [
    (category_sales, ["product_revenue", "freight_total"]),
    (seller_sales, ["product_revenue", "freight_total"]),
    (
        daily_sales,
        [
            "product_revenue",
            "freight_total",
            "payment_total",
            "average_review_score",
        ],
    ),
    (fact_items, ["item_price", "freight_value"]),
    (
        fact_orders,
        [
            "product_revenue",
            "freight_total",
            "payment_total",
            "average_review_score",
        ],
    ),
    (payments, ["payment_value"]),
]

for dataframe, columns in numeric_columns:
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce",
            )


# Convert date columns to Pandas datetime types.
# This enables .dt.date and date-range filtering.
date_columns = [
    (fact_orders, "purchase_date"),
    (fact_items, "purchase_date"),
    (daily_sales, "order_date"),
]

for dataframe, column in date_columns:
    if column in dataframe.columns:
        dataframe[column] = pd.to_datetime(
            dataframe[column],
            errors="coerce",
        )

# --------------------------------------------------
# SIDEBAR NAVIGATION
# --------------------------------------------------

st.sidebar.title("RetailFlow 2.0")
st.sidebar.caption("Multi-source Data Engineering Platform")

page = st.sidebar.radio(
    "Navigate",
    [
        "Platform Overview",
        "Product Gallery",
        "Olist Analytics",
        "Data Quality & Lineage",
    ],
)

status = pipeline_run.get("status", "not run")

st.sidebar.divider()
st.sidebar.write("**Latest pipeline run**")

if status == "success":
    st.sidebar.success("All stages succeeded")
elif status == "running":
    st.sidebar.info("Pipeline running")
else:
    st.sidebar.warning(f"Status: {status}")

if pipeline_run.get("run_id"):
    st.sidebar.caption(
        f"Run ID: {pipeline_run['run_id']}"
    )

if st.sidebar.button("Refresh dashboard data"):
    st.cache_data.clear()
    st.rerun()


# --------------------------------------------------
# PAGE 1: PLATFORM OVERVIEW
# --------------------------------------------------

if page == "Platform Overview":

    st.title("RetailFlow 2.0")
    st.subheader("Multi-source E-Commerce Data Platform")

    st.write(
        "A unified view of product catalogue ingestion, local image "
        "assets, marketplace analytics, data-quality checks, and "
        "pipeline execution history."
    )

    order_count = len(fact_orders)
    item_count = len(fact_items)
    customer_count = (
        customers["customer_unique_id"].nunique()
        if (
            not customers.empty
            and "customer_unique_id" in customers.columns
        )
        else 0
    )

    product_revenue = (
        pd.to_numeric(fact_items["item_price"], errors="coerce").sum()
        if not fact_items.empty
        else 0
    )

    image_count = (
        sum(
            1
            for path in image_manifest.get(
                "local_path", pd.Series(dtype=str)
            ).dropna()
            if (PROJECT_ROOT / Path(path)).is_file()
        )
        if not image_manifest.empty
        else 0
    )

    product_count = len(products)

    col1, col2, col3 = st.columns(3)

    col1.metric("Olist Orders", f"{order_count:,}")
    col2.metric("Order Items", f"{item_count:,}")
    col3.metric("Unique Marketplace Customers", f"{customer_count:,}")

    col4, col5, col6 = st.columns(3)

    col4.metric(
        "Product Revenue",
        format_currency(product_revenue),
    )
    col5.metric("Catalogue Products", f"{product_count:,}")
    col6.metric("Local Image Files", f"{image_count:,}")

    st.divider()

    st.markdown("### Platform Architecture")

    architecture = [
        (
            "Sources",
            "Online Retail, Olist relational tables, product API",
        ),
        (
            "Bronze",
            "Raw JSON snapshots and source tables in Parquet",
        ),
        (
            "Silver",
            "Typed, standardized data with quality checks",
        ),
        (
            "Gold",
            "Facts, dimensions, and business aggregates",
        ),
        (
            "Monitoring",
            "Run history, stage logs, data quality, reconciliation",
        ),
    ]

    for index in range(0, len(architecture), 2):
        columns = st.columns(2)

        for offset, column in enumerate(columns):
            item_index = index + offset

            if item_index >= len(architecture):
                continue

            title, description = architecture[item_index]

            with column:
                st.markdown(f"**{title}**")
                st.write(description)

    st.divider()

    st.markdown("### Marketplace Performance")

    left, right = st.columns(2)

    with left:
        st.markdown("**Daily Product Revenue**")

        if not daily_sales.empty:
            chart_data = daily_sales.copy()
            chart_data["order_date"] = pd.to_datetime(
                chart_data["order_date"]
            )

            chart_data = chart_data.sort_values("order_date")

            fig = px.line(
                chart_data,
                x="order_date",
                y="product_revenue",
                title="Olist Product Revenue Over Time",
                labels={
                    "order_date": "Order Date",
                    "product_revenue": "Product Revenue (R$)",
                },
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Gold daily sales data is unavailable.")

    with right:
        st.markdown("**Top Product Categories**")

        if not category_sales.empty:
            chart_data = category_sales.nlargest(
                12, "product_revenue"
            ).sort_values("product_revenue")

            fig = px.bar(
                chart_data,
                x="product_revenue",
                y="product_category",
                orientation="h",
                title="Leading Categories",
                labels={
                    "product_revenue": "Product Revenue (R$)",
                    "product_category": "Category",
                },
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Gold category data is unavailable.")

    st.divider()

    st.markdown("### Latest Execution")

    if pipeline_run:
        stage_data = pd.DataFrame(
            pipeline_run.get("stages", [])
        )

        if not stage_data.empty:
            visible_columns = [
                column
                for column in [
                    "name",
                    "status",
                    "duration_seconds",
                    "exit_code",
                ]
                if column in stage_data.columns
            ]

            st.dataframe(
                stage_data[visible_columns],
                use_container_width=True,
                hide_index=True,
            )

        st.caption(
            f"Run duration: "
            f"{pipeline_run.get('duration_seconds', 0):,.2f} seconds"
        )
    else:
        st.warning("No pipeline execution record was found.")


# --------------------------------------------------
# PAGE 2: PRODUCT GALLERY
# --------------------------------------------------

elif page == "Product Gallery":

    st.title("Product Catalogue & Image Gallery")

    st.write(
        "Browse products ingested from the product API. "
        "Images are served from the local catalogue-image directory "
        "whenever the file is available."
    )

    st.markdown(
        f"**Price display:** USD amounts converted to INR at "
        f"₹{USD_TO_INR_RATE:,.4f} per US$1 "
        f"(FBIL reference rate, {USD_TO_INR_RATE_AS_OF}; fixed, not live). "
        f"[Source: NSE India]({USD_TO_INR_SOURCE_URL})"
    )

    if products.empty:
        st.warning(
            "Product catalogue is missing. Run "
            "ingestion/ingest_product_catalog.py first."
        )
        st.stop()

    search_text = st.text_input(
        "Search by product, category or brand",
        placeholder="For example, mascara, beauty or phone",
    )

    categories = sorted(
        products["category"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    selected_category = st.selectbox(
        "Category",
        ["All categories"] + categories,
    )

    sort_order = st.selectbox(
        "Sort products",
        [
            "Name (A–Z)",
            "Price: Low to High",
            "Price: High to Low",
            "Rating: High to Low",
        ],
    )

    filtered = products.copy()

    if search_text.strip():
        search = search_text.strip().lower()

        search_columns = [
            column
            for column in ["title", "description", "category", "brand"]
            if column in filtered.columns
        ]

        mask = pd.Series(False, index=filtered.index)

        for column in search_columns:
            mask |= (
                filtered[column]
                .fillna("")
                .astype(str)
                .str.lower()
                .str.contains(search, regex=False)
            )

        filtered = filtered[mask]

    if selected_category != "All categories":
        filtered = filtered[
            filtered["category"] == selected_category
        ]

    if sort_order == "Name (A–Z)":
        filtered = filtered.sort_values("title")
    elif sort_order == "Price: Low to High":
        filtered = filtered.sort_values("price")
    elif sort_order == "Price: High to Low":
        filtered = filtered.sort_values(
            "price", ascending=False
        )
    else:
        filtered = filtered.sort_values(
            "rating", ascending=False
        )

    st.caption(
        f"Showing {len(filtered):,} products "
        f"from {len(products):,} catalogue records."
    )

    page_size = 20
    max_pages = max(
        1,
        (len(filtered) + page_size - 1) // page_size,
    )

    current_page = st.number_input(
        "Page",
        min_value=1,
        max_value=max_pages,
        value=1,
        step=1,
    )

    start = (int(current_page) - 1) * page_size
    page_products = filtered.iloc[start:start + page_size]

    for row_start in range(0, len(page_products), 4):
        columns = st.columns(4)
        block = page_products.iloc[row_start:row_start + 4]

        for column, (_, product) in zip(columns, block.iterrows()):
            with column:
                product_id = str(product["source_product_id"])
                local_path = thumbnail_map.get(product_id)

                show_local_image(
                    local_path,
                    product.get("thumbnail_url"),
                )

                st.markdown(f"**{product.get('title', 'Untitled')}**")
                st.caption(
                    f"{product.get('category', 'Uncategorised')}"
                )

                price = pd.to_numeric(
                    product.get("price"), errors="coerce"
                )

                price_inr = convert_usd_to_inr(price)

                if price_inr is not None:
                    st.write(f"**₹{price_inr:,.2f}**")

                rating = product.get("rating")

                if pd.notna(rating):
                    st.caption(f"Rating: {float(rating):.1f}/5")

                stock = product.get("stock")

                if pd.notna(stock):
                    st.caption(f"Stock: {int(stock):,}")

                st.caption(f"Product ID: {product_id}")

                with st.expander("Product details"):
                    st.write(
                        product.get("description", "No description")
                    )

                    urls = product.get("image_urls_json", "[]")

                    try:
                        image_urls = json.loads(urls)
                        st.write(
                            f"Available image URLs: {len(image_urls)}"
                        )
                    except (ValueError, TypeError):
                        pass

        st.divider()


# --------------------------------------------------
# PAGE 3: OLIST MARKETPLACE ANALYTICS
# --------------------------------------------------

elif page == "Olist Analytics":

    st.title("Olist Marketplace Analytics")

    st.caption(
        "Metrics in this section use the Olist Brazilian "
        "e-commerce dataset. Currency is displayed in Brazilian reais (R$)."
    )

    if fact_orders.empty or fact_items.empty:
        st.warning(
            "Gold facts are missing. Run etl/build_olist_gold.py first."
        )
        st.stop()

    orders = fact_orders.copy()
    items = fact_items.copy()

    orders["purchase_date"] = pd.to_datetime(
        orders["purchase_date"], errors="coerce"
    )

    if daily_sales.empty:
        st.warning("Gold daily sales is unavailable.")
        st.stop()

    daily = daily_sales.copy()
    daily["order_date"] = pd.to_datetime(
        daily["order_date"], errors="coerce"
    )

    min_date = daily["order_date"].min().date()
    max_date = daily["order_date"].max().date()

    date_range = st.date_input(
        "Filter purchase dates",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if len(date_range) == 2:
        start_date, end_date = date_range

        orders = orders[
            orders["purchase_date"].dt.date.between(
                start_date, end_date
            )
        ]

        items = items[
            items["purchase_date"].dt.date.between(
                start_date, end_date
            )
        ]

        daily = daily[
            daily["order_date"].dt.date.between(
                start_date, end_date
            )
        ]

    # Keep payment-method totals in sync with the selected order-date range.
    if not payments.empty:
        selected_order_ids = set(
            orders["order_id"].dropna().astype(str)
        )
        filtered_payments = payments[
            payments["order_id"].astype(str).isin(selected_order_ids)
        ].copy()
    else:
        filtered_payments = payments.copy()

    total_orders = len(orders)
    total_items = len(items)

    revenue = pd.to_numeric(
        items["item_price"], errors="coerce"
    ).sum()

    freight = pd.to_numeric(
        items["freight_value"], errors="coerce"
    ).sum()

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Orders", f"{total_orders:,}")
    col2.metric("Order Items", f"{total_items:,}")
    col3.metric("Product Revenue", format_currency(revenue))
    col4.metric("Freight Value", format_currency(freight))

    st.divider()

    left, right = st.columns(2)

    with left:
        st.markdown("### Daily Orders")

        daily_orders = daily.sort_values("order_date")

        fig = px.line(
            daily_orders,
            x="order_date",
            y="order_count",
            markers=True,
            labels={
                "order_date": "Date",
                "order_count": "Orders",
            },
        )

        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("### Order Status")

        if "order_status" in orders.columns:
            statuses = (
                orders["order_status"]
                .fillna("unknown")
                .value_counts()
                .rename_axis("order_status")
                .reset_index(name="orders")
            )

            fig = px.bar(
                statuses,
                x="order_status",
                y="orders",
                labels={
                    "order_status": "Order Status",
                    "orders": "Orders",
                },
            )

            st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.markdown("### Revenue by Product Category")

        if not category_sales.empty:
            top_categories = category_sales.nlargest(
                15, "product_revenue"
            ).sort_values("product_revenue")

            fig = px.bar(
                top_categories,
                x="product_revenue",
                y="product_category",
                orientation="h",
                labels={
                    "product_revenue": "Product Revenue (R$)",
                    "product_category": "Category",
                },
            )

            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("### Top Sellers")

        if not seller_sales.empty:
            top_sellers = seller_sales.nlargest(
                10, "product_revenue"
            ).sort_values("product_revenue")

            top_sellers = top_sellers.copy()
            top_sellers["seller_label"] = (
                top_sellers["seller_id"].astype(str)
                + " — "
                + top_sellers["seller_city"].fillna("Unknown")
            )

            fig = px.bar(
                top_sellers,
                x="product_revenue",
                y="seller_label",
                orientation="h",
                labels={
                    "product_revenue": "Product Revenue (R$)",
                    "seller_label": "Seller",
                },
            )

            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.markdown("### Payment Methods")

    if not filtered_payments.empty:
        payment_summary = (
            filtered_payments.groupby("payment_type", as_index=False)
            .agg(
                payment_records=("order_id", "count"),
                payment_value=("payment_value", "sum"),
            )
            .sort_values("payment_value", ascending=False)
        )

        fig = px.bar(
            payment_summary,
            x="payment_type",
            y="payment_value",
            labels={
                "payment_type": "Payment Method",
                "payment_value": "Payment Value (R$)",
            },
        )

        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            payment_summary,
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("View daily sales table"):
        st.dataframe(
            daily.sort_values("order_date"),
            use_container_width=True,
            hide_index=True,
        )


# --------------------------------------------------
# PAGE 4: DATA QUALITY AND LINEAGE
# --------------------------------------------------

elif page == "Data Quality & Lineage":

    st.title("Data Quality & Pipeline Lineage")

    st.write(
        "Inspect the latest orchestration run, source-table counts, "
        "Silver/Gold validation results and product-image ingestion."
    )

    if pipeline_run:
        st.markdown("### Latest Pipeline Execution")

        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Pipeline Status",
            str(pipeline_run.get("status", "unknown")).upper(),
        )

        col2.metric(
            "Stages",
            (
                f"{sum(s.get('status') == 'success' for s in pipeline_run.get('stages', []))}"
                f"/{pipeline_run.get('stage_count', 0)}"
            ),
        )

        col3.metric(
            "Duration",
            f"{pipeline_run.get('duration_seconds', 0):,.2f} sec",
        )

        st.caption(
            f"Run ID: {pipeline_run.get('run_id', 'Unknown')}"
        )

        stages = pd.DataFrame(
            pipeline_run.get("stages", [])
        )

        if not stages.empty:
            st.markdown("#### Stage Execution Details")

            preferred = [
                column
                for column in [
                    "name",
                    "status",
                    "started_at_utc",
                    "finished_at_utc",
                    "duration_seconds",
                    "exit_code",
                ]
                if column in stages.columns
            ]

            st.dataframe(
                stages[preferred],
                use_container_width=True,
                hide_index=True,
            )

        lineage = pipeline_run.get("lineage", {})

        st.markdown("#### Run Artifacts")

        for label, path in lineage.items():
            st.write(f"**{label.replace('_', ' ').title()}:** `{path}`")

    else:
        st.warning("No pipeline lineage record was found.")

    st.divider()

    st.markdown("### Bronze Ingestion")

    if bronze_report:
        bronze_tables = pd.DataFrame(
            bronze_report.get("tables", [])
        )

        if not bronze_tables.empty:
            visible = [
                column
                for column in [
                    "source_file",
                    "row_count",
                    "column_count",
                    "blank_cells",
                    "expected_schema_valid",
                    "status",
                ]
                if column in bronze_tables.columns
            ]

            st.dataframe(
                bronze_tables[visible],
                use_container_width=True,
                hide_index=True,
            )

    else:
        st.info("Bronze manifest is unavailable.")

    st.divider()

    st.markdown("### Silver Quality Checks")

    if silver_report:
        checks = silver_report.get("checks", {})

        checks_df = pd.DataFrame([
            {
                "check": name,
                "passed": passed,
            }
            for name, passed in checks.items()
        ])

        st.dataframe(
            checks_df,
            use_container_width=True,
            hide_index=True,
        )

        st.write("**Table row counts**")
        st.json(silver_report.get("row_counts", {}))

        if silver_report.get("key_quality"):
            with st.expander("Review primary/composite key checks"):
                st.json(silver_report["key_quality"])

        if silver_report.get("foreign_key_quality"):
            with st.expander("Review foreign-key checks"):
                st.json(silver_report["foreign_key_quality"])

    else:
        st.info("Silver quality report is unavailable.")

    st.divider()

    st.markdown("### Gold Warehouse Quality Checks")

    if gold_report:
        checks = gold_report.get("checks", {})

        checks_df = pd.DataFrame([
            {
                "check": name,
                "passed": passed,
            }
            for name, passed in checks.items()
        ])

        st.dataframe(
            checks_df,
            use_container_width=True,
            hide_index=True,
        )

        st.write("**Revenue and row-count reconciliation**")
        st.json(gold_report.get("reconciliation", {}))

    else:
        st.info("Gold quality report is unavailable.")

    st.divider()

    st.markdown("### Product Catalogue and Image Quality")

    if catalog_report:
        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Products Ingested",
            f"{catalog_report.get('records_received', 0):,}",
        )

        col2.metric(
            "Product Categories",
            f"{catalog_report.get('distinct_categories', 0):,}",
        )

        col3.metric(
            "Catalog Quality",
            "PASSED" if catalog_report.get("passed") else "FAILED",
        )

        st.json(catalog_report.get("checks", {}))

    if not image_manifest.empty:
        image_summary = (
            image_manifest.groupby(
                "download_status",
                dropna=False,
            )
            .size()
            .reset_index(name="associations")
        )

        st.write("**Image download statuses**")

        st.dataframe(
            image_summary,
            use_container_width=True,
            hide_index=True,
        )

        local_count = sum(
            (PROJECT_ROOT / Path(path)).is_file()
            for path in image_manifest["local_path"].dropna()
        )

        st.metric(
            "Available Local Image Files",
            f"{local_count:,}",
        )

    st.divider()

    st.markdown("### Run History")

    history_file = (
        OUTPUT_DIR / "lineage" / "pipeline_runs.jsonl"
    )

    if history_file.is_file():
        history_rows = []

        with history_file.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line in file:
                if line.strip():
                    try:
                        row = json.loads(line)
                        history_rows.append({
                            "run_id": row.get("run_id"),
                            "status": row.get("status"),
                            "started_at_utc": row.get("started_at_utc"),
                            "duration_seconds": row.get("duration_seconds"),
                            "stages_completed": sum(
                                s.get("status") == "success"
                                for s in row.get("stages", [])
                            ),
                            "stage_count": row.get("stage_count"),
                        })
                    except json.JSONDecodeError:
                        continue

        if history_rows:
            history_df = pd.DataFrame(history_rows)
            st.dataframe(
                history_df.iloc[::-1].head(20),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No readable run-history entries found.")

    else:
        st.info("No pipeline run history is available yet.")

    st.caption(
        "These monitoring records are generated by the local "
        "RetailFlow 2.0 orchestrator."
    )
