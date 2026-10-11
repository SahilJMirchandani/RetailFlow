# RetailFlow — E-Commerce Data Engineering Platform

**RetailFlow** is a Data Engineering mini project that demonstrates data ingestion, layered ETL, data quality, analytical modelling, orchestration, data lineage, forecasting, and dashboard visualisation.

The repository contains **two separate data workflows**. They share the project but not the same source data or currency.

## 1. Project tracks

### Track A — Original Online Retail pipeline

This is the original UK-retailer workflow:

```text
Online Retail.xlsx
      ↓
Ingestion and validation
      ↓
Cleaning and transformation
      ↓
PostgreSQL: retailflow.sales_transactions
      ↓
Business analytics + 30-day revenue forecast
      ↓
Streamlit dashboard
```

The recorded run processed **541,909 raw rows** and produced **524,878 cleaned transactions**. Its analytics are denominated in **British pounds (£)**.

Recorded business metrics:

| Metric | Recorded result |
|---|---:|
| Cleaned transactions | 524,878 |
| Revenue | £10,642,110.80 |
| Orders | 19,960 |
| Unique customers | 4,338 |
| Average order value | £533.17 |
| Forecast horizon | 30 days |

The metrics above describe the saved run and may change if the input dataset or pipeline is changed.

### Track B — RetailFlow 2.0 multi-source data platform

The V2 workflow adds an external product catalogue, local product-image ingestion, and the Brazilian Olist marketplace dataset. It implements the **Bronze → Silver → Gold** pattern for Olist data.

```text
DummyJSON product API ──→ Bronze API snapshot
         │                       ↓
         ├──→ Silver product catalogue ──→ product-image downloader
         │
Olist CSV files ──→ Bronze Parquet
                         ↓
                    Silver tables
                         ↓
            Gold dimensions + fact tables
                         ↓
          Business sales aggregates + quality reports
```

The orchestrator runs five stages in order:

1. Product Catalogue API Ingestion
2. Product Image Ingestion
3. Olist Bronze Ingestion
4. Olist Silver Transformation
5. Olist Gold Warehouse

A stage failure stops the sequence. The V2 workflow writes timestamped logs and a latest-run record plus append-only JSONL history to `data/output/`.

**Important:** DummyJSON's sample product IDs and Olist product IDs belong to different sources. RetailFlow does not treat them as matching keys. Olist measures remain in Brazilian reais (R$); the original Online Retail track remains in pounds (£). Product-gallery INR is a display conversion only and does not rewrite source data.

## 2. V2 data model

Olist's nine CSV files are ingested to Bronze Parquet, standardized and validated in Silver, and transformed to Gold Parquet tables.

| Gold table | Grain / purpose |
|---|---|
| `dim_customers` | Customer dimension |
| `dim_products` | Product dimension |
| `dim_sellers` | Seller dimension |
| `dim_date` | Calendar dimension |
| `fact_orders` | One row per order |
| `fact_order_items` | One row per order-item |
| `gold_daily_sales` | Daily business aggregates |
| `gold_category_sales` | Category aggregates |
| `gold_seller_performance` | Seller aggregates |

Quality checks include schema and row-count validation, duplicate-key checks, dimension-key checks, fact-to-dimension relationship checks, row-count reconciliation, and revenue reconciliation across fact and aggregate outputs.

Product catalogue ingestion retrieves records from the [DummyJSON products API](https://dummyjson.com/products). The image downloader stores image files locally and records their source URLs and local paths in a manifest. Image files and downloaded datasets are intentionally excluded from Git.

## 3. Repository layout

```text
RetailFlow/
├── dashboard/
│   ├── app.py                     # Original dashboard
│   └── retailflow_v2_app.py       # V2 product/Olist dashboard
├── ingestion/
│   ├── ingest_data.py             # Original workbook ingestion
│   ├── ingest_product_catalog.py   # Product API ingestion
│   ├── download_product_images.py # Local catalogue images
│   └── ingest_olist.py            # Olist CSV → Bronze Parquet
├── etl/
│   ├── ...                        # Original ETL modules
│   ├── transform_olist_silver.py  # Bronze → Silver
│   └── build_olist_gold.py        # Silver → Gold
├── pipeline/
│   ├── run_pipeline.py            # Original six-stage workflow
│   └── run_retailflow_v2.py       # V2 five-stage orchestrator
├── tests/                         # Unit and data-dependent checks
├── data/
│   ├── raw/                       # Local input data (not committed)
│   ├── bronze/                    # V2 raw/layered outputs (not committed)
│   ├── silver/
│   ├── gold/
│   ├── processed/
│   ├── output/                    # Reports, forecasts and run lineage
│   └── catalog_images/            # Downloaded product images
├── requirements.txt
└── README.md
```

The exact local folder contents vary by which workflows have been run. Generated datasets, images, forecasts, logs, reports, and local credentials are excluded from Git.

## 4. Setup

Use Python 3.11 or 3.13. CI tests both versions.

### Windows PowerShell

```powershell
git clone https://github.com/SahilJMirchandani/RetailFlow.git
cd RetailFlow

python -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks environment activation, use Command Prompt instead:

```bat
.venv\Scripts\activate.bat
```

## 5. Run Track A — Original Online Retail

Obtain the Online Retail workbook separately. It is not stored in this repository.

The simplest location is:

```text
data/raw/Online Retail.xlsx
```

Then run:

```powershell
python pipeline/run_pipeline.py
streamlit run dashboard/app.py
```

The ingestion script also supports an alternate workbook location using the `RETAILFLOW_ONLINE_RETAIL_SOURCE` environment variable. For example, in PowerShell for the current terminal session:

```powershell
$env:RETAILFLOW_ONLINE_RETAIL_SOURCE = "C:\path\to\Online Retail.xlsx"
python pipeline/run_pipeline.py
```

The original workflow loads to PostgreSQL. Copy the safe template and edit the local values:

```powershell
Copy-Item .env.example .env
```

Update `.env` with your own PostgreSQL credentials. Make sure the PostgreSQL server and `retailflow` database exist before running the pipeline. The loader reads `.env` locally. Never commit passwords or the real `.env` file.

## 6. Run Track B — RetailFlow 2.0

### Prepare Olist inputs

Download the public [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) separately. Create `data/raw/olist/` and place the nine CSV files there with these exact filenames:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

The input data is not included in the repository. The folder and its contents should remain local.

### Run the V2 orchestrator

From the project root, with your virtual environment activated and an internet connection for the product API and image downloads:

```powershell
python pipeline/run_retailflow_v2.py
```

The orchestrator runs the five stages in sequence. It returns a non-zero exit code when the run does not succeed. Inspect the run log and the reports under `data/output/` if it stops.

### Launch the V2 dashboard

```powershell
streamlit run dashboard/retailflow_v2_app.py
```

The V2 dashboard presents Platform Overview, Product Gallery, Olist Analytics, and Data Quality & Lineage. It reads generated local outputs; run the V2 pipeline first to populate its data files. The dashboard is separate from `dashboard/app.py`.

## 7. Tests and continuous integration

Run tests from the repository root:

```powershell
python -m compileall -q dashboard ingestion etl pipeline tests
python -m pytest -q
```

Self-contained tests cover dataset-path resolution, ingestion behavior, stage execution, run-history writing, and exclusion of stale metrics from stages that did not succeed.

`tests/test_olist_integration.py` creates nine tiny synthetic Olist CSVs inside a temporary directory and runs the **real** Bronze ingestion, Silver transformation, and Gold warehouse functions. It checks quality reports, key relationships, row counts, revenue reconciliation, unchanged source files, stable Gold outputs over a second run, and rejection of a deliberately duplicated customer key. It does not read, overwrite, or require the full local Olist dataset.

The legacy tests in `tests/test_pipeline.py` validate outputs generated by Track A. They are skipped when the local cleaned output is unavailable; run Track A first to exercise them. The full five-stage V2 orchestration and network-dependent product/image ingestion still require local Olist input files and internet access, so they are not run by CI.

GitHub Actions runs Python compilation and the test suite on pushes to the main and feature branches and for pull requests to main. CI uses synthetic data and does not download or commit the real datasets.

## 8. Important data and security notes

- Do not commit `.env`, passwords, access tokens, or database credentials.
- Input datasets and generated Bronze/Silver/Gold Parquet, CSV outputs, logs, reports, and product images are intentionally ignored by Git.
- The DummyJSON catalogue is a sample API, not a canonical retail inventory.
- The Product Gallery converts sample USD prices to INR for display only using the fixed FBIL reference rate **₹96.7733 per US$1 (8 October 2026)**. The dashboard links to the [NSE India reference-rate page](https://www.nseindia.com/report-detail/rbi-reference-rate-statistics). The rate is not live and is not written back to source data.
- Do not compare or combine revenue across the UK-retailer and Brazilian-marketplace tracks without explicitly accounting for their different currencies and datasets.

## 9. Technology stack

Python · pandas · NumPy · PyArrow/Parquet · DuckDB · PostgreSQL · scikit-learn · Streamlit · Plotly · Requests · Pytest · GitHub Actions

## 10. Next improvements

Potential later enhancements include incremental ingestion, snapshot freshness checks, schema versioning, a more formal data-contract layer, Docker packaging, deployment, and scheduled execution. They are intentionally separate from the current scope so the existing workflows stay understandable and reproducible.
