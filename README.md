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

The orchestrator runs five local stages in order:

1. Product Catalogue API Ingestion
2. Product Image Ingestion
3. Olist Bronze Ingestion
4. Olist Silver Transformation
5. Olist Gold Warehouse

When `RETAILFLOW_AWS_ENABLED=true` is set, a sixth stage publishes the completed Bronze/Silver/Gold Parquet layers and selected quality/lineage metadata to an existing Amazon S3 bucket, then registers the nine Gold tables in the AWS Glue Data Catalog. The local stages remain unchanged and are still the source of truth.

A stage failure stops the sequence. The V2 workflow writes timestamped logs and a latest-run record plus append-only JSONL history to `data/output/`.

**Important:** DummyJSON's sample product IDs and Olist product IDs belong to different sources. RetailFlow does not treat them as matching keys. Olist source data and ETL quality reports remain in Brazilian reais (BRL); the Olist dashboard converts monetary values to INR for display only at **₹17.4612 per R$1 (2 November 2026)**. The Product Gallery separately converts sample USD prices to INR at the documented fixed rate. The original Online Retail track remains in pounds (£). Display conversions never rewrite stored source data.

## 2. V2 data model

Olist's nine CSV files are ingested to Bronze Parquet, standardized and validated in Silver, and transformed to Gold Parquet tables. Silver outputs are written to a temporary staging directory and published only after blocking quality checks pass; a failed write or rejected quality run preserves the prior published Silver layer.

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

After rerunning the V2 orchestrator, use **Refresh dashboard data** in the Streamlit sidebar to clear the dashboard cache and read the newly generated files. This button refreshes the dashboard only; it does not start the ETL pipeline.

## 7. Optional AWS integration — S3, Glue Data Catalog and Athena

RetailFlow can publish its completed V2 data lake to your existing AWS Academy Learner Lab environment. **No new AWS account is required.** The integration is opt-in, uses the AWS SDK credential chain, and does not create a bucket, EC2 instance, Glue crawler, Glue ETL job, or long-running compute service.

### AWS architecture

- **Amazon S3:** stores the existing Bronze, Silver and Gold Parquet layers plus selected quality reports and pipeline lineage. Uploads request S3-managed server-side encryption (AES256). Raw input CSV files and local `.env` credentials are excluded. Product images are excluded by default.
- **AWS Glue Data Catalog:** creates or updates a database and registers the nine Olist Gold Parquet tables as external tables.
- **Amazon Athena:** runs read-only SQL queries against the Glue tables. Query results are written to the configured S3 results prefix.

The local DuckDB transformations and dashboard continue to work without AWS. When AWS is enabled, the V2 orchestrator adds an AWS publication stage after the five local stages. The dashboard's Data Quality & Lineage page displays the latest local AWS publication receipt; it does not poll AWS on every page load.

### Configure your existing VESIT AWS Academy Learner Lab

1. Start the AWS Academy Learner Lab using your VES ID and wait for the lab status to become active. Open **AWS Details → AWS CLI**. Use the temporary access-key ID, secret access key and session token from that panel to configure the AWS CLI credential provider on your Windows machine (for example, the `[default]` profile in `%USERPROFILE%\\.aws\\credentials`, or the environment variables in the same PowerShell session). Do not paste the raw INI profile block directly as a PowerShell command. Do not send the keys in chat or put them in source files, screenshots, or `.env`. The credentials expire with the lab session and must be refreshed when the lab restarts.
2. Confirm the terminal is authenticated:

   ```powershell
   aws sts get-caller-identity
   ```

   Do not continue until this succeeds. If you are using CloudShell, note that CloudShell's session is separate from your Windows terminal; use the lab-provided temporary CLI credentials locally when running RetailFlow on Windows.
3. Use an **existing S3 bucket** that belongs to your Learner Lab account. Set its region correctly. This integration does not create a bucket automatically. If you do not already have one, create a single bucket from the S3 console only if your Learner Lab permissions allow it.
4. From the RetailFlow project root, copy the safe template and edit the AWS settings:

   ```powershell
   Copy-Item .env.example .env
   ```

   Keep your existing PostgreSQL values if you use Track A. Set the following values in `.env`:

   ```dotenv
   RETAILFLOW_AWS_ENABLED=true
   RETAILFLOW_AWS_REGION=ap-south-1
   RETAILFLOW_AWS_BUCKET=your-existing-bucket-name
   RETAILFLOW_AWS_PREFIX=retailflow
   RETAILFLOW_GLUE_DATABASE=retailflow
   RETAILFLOW_ATHENA_WORKGROUP=primary
   RETAILFLOW_ATHENA_OUTPUT_LOCATION=
   RETAILFLOW_AWS_UPLOAD_IMAGES=false
   ```

   Replace the bucket name and set the region to the actual bucket region. Leave the Athena output location blank to use `s3://<bucket>/retailflow/athena-results/`. Do not place access keys or session tokens in `.env`.
5. Install the AWS SDK dependency and run the normal V2 pipeline:

   ```powershell
   .\.venv\Scripts\python.exe -m pip install -r requirements.txt
   .\.venv\Scripts\python.exe pipeline\run_retailflow_v2.py
   ```

   After the five local stages succeed, the optional sixth stage uploads Parquet and metadata objects, verifies uploaded object sizes, and registers Gold tables in Glue. A failed AWS publication is recorded as a failed pipeline run rather than silently reported as success.
6. Confirm the publication receipt at `data/output/aws_publication_latest.json` and inspect the S3 bucket. The dashboard's **Data Quality & Lineage → AWS Data Lake** section also shows the latest successful publication receipt.
7. Run the sample Athena query:

   ```powershell
   .\.venv\Scripts\python.exe -m aws.athena_query
   ```

   It runs a read-only query against `gold_daily_sales`. You can supply another read-only query with `--sql`, for example:

   ```powershell
   .\.venv\Scripts\python.exe -m aws.athena_query --sql "SELECT * FROM gold_category_sales LIMIT 10"
   ```

### IAM permissions and cost control

The Learner Lab role must allow `sts:GetCallerIdentity`; S3 `s3:ListBucket` and `s3:GetBucketLocation` on the bucket, plus `s3:GetObject` and `s3:PutObject` on the RetailFlow prefix; Glue `GetDatabase`, `CreateDatabase`, `GetTable`, `CreateTable` and `UpdateTable`; and Athena `GetWorkGroup`, `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults` and `StopQueryExecution`. The Athena results prefix must also be writable/readable by the role. If the lab role denies a service, the command reports the AWS error; do not try to bypass Learner Lab restrictions.

S3 storage and Athena scans can incur charges against the Learner Lab budget/credits. This project does not claim that AWS usage is free. Keep the image upload option disabled, query Parquet Gold tables with a small `LIMIT`, avoid repeated full scans, and remove test outputs from S3 when the assignment is finished and the lab policy permits it. No always-on compute service is provisioned.

## 8. Tests and continuous integration

Run tests from the repository root:

```powershell
python -m compileall -q aws dashboard ingestion etl pipeline tests
python -m pytest -q
```

Self-contained tests cover dataset-path resolution, ingestion behavior, stage execution, run-history writing, and exclusion of stale metrics from stages that did not succeed.

`tests/test_olist_integration.py` creates nine tiny synthetic Olist CSVs inside a temporary directory and runs the **real** Bronze ingestion, Silver transformation, and Gold warehouse functions. It checks quality reports, key relationships, row counts, revenue reconciliation, unchanged source files, stable Gold outputs over a second run, and rejection of a deliberately duplicated customer key. It does not read, overwrite, or require the full local Olist dataset. It also injects a Silver write failure and verifies that all previously published Silver Parquet files remain byte-for-byte unchanged and that staging artifacts are cleaned up.

The legacy tests in `tests/test_pipeline.py` validate outputs generated by Track A. They are skipped when the local cleaned output is unavailable; run Track A first to exercise them. The full five-stage V2 orchestration and network-dependent product/image ingestion still require local Olist input files and internet access, so they are not run by CI.

GitHub Actions runs Python compilation and the test suite on pushes to the main and feature branches and for pull requests to main. CI uses synthetic data and does not download or commit the real datasets.

## 9. Important data and security notes

- Do not commit `.env`, passwords, access tokens, or database credentials.
- Input datasets and generated Bronze/Silver/Gold Parquet, CSV outputs, logs, reports, and product images are intentionally ignored by Git.
- The DummyJSON catalogue is a sample API, not a canonical retail inventory.
- The Product Gallery converts sample USD prices to INR for display only using the fixed FBIL reference rate **₹96.7733 per US$1 (8 October 2026)**. The dashboard links to the [NSE India reference-rate page](https://www.nseindia.com/report-detail/rbi-reference-rate-statistics). The rate is not live and is not written back to source data.
- Olist dashboard and analytics monetary values are converted from BRL to INR at the pinned historical rate **₹17.4612 per R$1 (2 November 2026)**. See [Mataf historical BRL/INR data](https://www.mataf.net/en/currency/converter-BRL-INR/data). This is a dated display rate, not a live quote; raw Olist datasets and generated quality reports remain in BRL.
- Do not compare or combine revenue across the UK-retailer and Brazilian-marketplace tracks without explicitly accounting for their different currencies and datasets.

## 10. Technology stack

Python · pandas · NumPy · PyArrow/Parquet · DuckDB · PostgreSQL · scikit-learn · Streamlit · Plotly · Requests · boto3 · Amazon S3 · AWS Glue Data Catalog · Amazon Athena · Pytest · GitHub Actions

## 11. Next improvements

Potential later enhancements include incremental ingestion, snapshot freshness checks, schema versioning, a more formal data-contract layer, Docker packaging, deployment, and scheduled execution. They are intentionally separate from the current scope so the existing workflows stay understandable and reproducible.
