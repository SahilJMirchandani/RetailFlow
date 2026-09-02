# RetailFlow: E-Commerce Sales Data Pipeline

A prototype Data Engineering platform demonstrating how an automated e-commerce data pipeline can transform raw transactional data into actionable business insights and revenue forecasts.

## 📌 Project Overview

**RetailFlow** is a Data Engineering mini project built around the **Online Retail transactional dataset**.

The project demonstrates an end-to-end data pipeline that:

* Ingests raw transactional data
* Validates data quality
* Cleans and transforms the dataset
* Stores processed transactions in PostgreSQL
* Generates business analytics
* Forecasts future revenue using Machine Learning
* Displays insights through an interactive Streamlit dashboard
* Automates the complete workflow through a pipeline orchestrator
* Performs automated testing using Pytest

The primary focus of the project is **Data Engineering**, while revenue forecasting is implemented as a downstream analytical component.

---

## 🎯 Objectives

1. Build an end-to-end e-commerce data pipeline.
2. Perform data ingestion and validation on raw transactional data.
3. Clean and transform large-scale transactional records.
4. Store structured data in a PostgreSQL database.
5. Generate meaningful business analytics from the stored data.
6. Implement revenue forecasting using a Random Forest model.
7. Develop an interactive Streamlit dashboard.
8. Automate the complete pipeline using a Python pipeline orchestrator.
9. Implement automated tests and error handling.

---

## 🏗️ System Architecture

```text
                    ┌──────────────────────┐
                    │   Online Retail.xlsx │
                    │      Raw Dataset      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Data Ingestion      │
                    │ ingest_data.py        │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Data Validation     │
                    │ validate_data.py      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ ETL / Transformation  │
                    │ transform_data.py     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │     PostgreSQL        │
                    │      retailflow       │
                    │ sales_transactions    │
                    └──────────┬───────────┘
                               │
                  ┌────────────┴────────────┐
                  ▼                         ▼
       ┌──────────────────┐       ┌──────────────────┐
       │    Analytics     │       │   Forecasting    │
       │ Business Metrics │       │ Random Forest ML │
       └────────┬─────────┘       └────────┬─────────┘
                │                          │
                └────────────┬─────────────┘
                             ▼
                  ┌──────────────────────┐
                  │ Streamlit Dashboard  │
                  │ Interactive Insights │
                  └──────────────────────┘
```

---

## 🔄 Data Pipeline

The complete automated workflow consists of six major stages:

```text
Data Ingestion
      ↓
Data Validation
      ↓
ETL / Transformation
      ↓
PostgreSQL Loading
      ↓
Analytics Generation
      ↓
Revenue Forecasting
```

The pipeline stops automatically if a stage encounters an error.

---

## 📊 Dataset

The project uses the **Online Retail dataset**, containing transactional records from an e-commerce retailer.

### Raw Dataset

* Records: **541,909**
* Columns: **8**
* File format: Excel (`.xlsx`)

The raw dataset is intentionally **not included in this repository** because of its size.

Place the dataset at:

```text
data/raw/Online Retail.xlsx
```

The ingestion module can also copy the dataset from the configured source location into the project's raw data directory.

---

## 🧹 Data Validation

The validation stage checks:

* Dataset schema
* Missing values
* Duplicate records
* Invalid quantities
* Invalid prices
* Invalid invoice dates
* Cancelled transactions

### Validation Results

| Check                    |  Result |
| ------------------------ | ------: |
| Raw records              | 541,909 |
| Missing values           | 136,534 |
| Duplicate records        |   5,268 |
| Invalid quantity records |  10,624 |
| Invalid price records    |   2,517 |
| Invalid date records     |       0 |
| Cancelled transactions   |   9,288 |

A validation report is generated in:

```text
data/output/validation_report.txt
```

---

## 🔧 ETL and Transformation

The ETL stage performs:

* Duplicate removal
* Cancellation filtering
* Invalid quantity removal
* Invalid price removal
* Date conversion
* Revenue calculation
* Daily revenue aggregation

### ETL Results

| Metric             |   Value |
| ------------------ | ------: |
| Initial records    | 541,909 |
| Cleaned records    | 524,878 |
| Daily revenue rows |     305 |

Processed files:

```text
data/processed/cleaned_transactions.csv
data/processed/daily_revenue.csv
```

---

## 🗄️ PostgreSQL Database

The cleaned transactional data is stored in PostgreSQL.

### Database

```text
Database: retailflow
Table: sales_transactions
Port: 5432
```

### Main Table Structure

| Column           | Type               |
| ---------------- | ------------------ |
| transaction_id   | SERIAL PRIMARY KEY |
| invoice_no       | VARCHAR            |
| stock_code       | VARCHAR            |
| description      | TEXT               |
| quantity         | INTEGER            |
| invoice_date     | TIMESTAMP          |
| unit_price       | NUMERIC            |
| customer_id      | NUMERIC            |
| country          | VARCHAR            |
| revenue          | NUMERIC            |
| transaction_date | DATE               |

### Database Verification

```text
Records loaded: 524,878
Database records: 524,878
```

The database loader is designed to avoid duplicate loading during repeated pipeline executions.

---

## 📈 Analytics

The analytics module generates business insights from PostgreSQL data.

Generated outputs include:

```text
revenue_by_country.csv
top_products.csv
daily_revenue.csv
monthly_revenue.csv
```

### Final Business Metrics

| Metric              |          Value |
| ------------------- | -------------: |
| Total Records       |        524,878 |
| Total Quantity Sold |      5,572,420 |
| Total Revenue       | £10,642,110.80 |
| Unique Customers    |          4,338 |
| Total Orders        |         19,960 |
| Average Order Value |        £533.17 |

The analytics stage helps identify:

* Revenue trends
* Top-performing countries
* Top-selling products
* Monthly revenue patterns
* Overall sales performance

---

## 🤖 Revenue Forecasting

RetailFlow includes a downstream revenue forecasting component using **Random Forest Regression**.

### Features

The model uses:

* Day of week
* Day of month
* Month
* Week of year
* Lag 1 day
* Lag 7 days
* Lag 14 days
* 7-day rolling revenue mean

### Model Configuration

```text
Algorithm: Random Forest Regressor
Estimators: 100
Maximum Depth: 10
Minimum Samples Leaf: 4
Minimum Samples Split: 2
Random State: 42
```

### Evaluation Results

| Metric |    Result |
| ------ | --------: |
| MAE    | 15,271.11 |
| RMSE   | 25,319.01 |
| R²     |    0.3375 |

The system generates a **30-day future revenue forecast**.

Output:

```text
data/output/revenue_forecast.csv
```

---

## 📊 Streamlit Dashboard

RetailFlow provides an interactive dashboard built using Streamlit and Plotly.

### Dashboard Features

* Date-range filtering
* Country filtering
* Total revenue KPI
* Total orders KPI
* Units sold KPI
* Customer count KPI
* Average order value
* Revenue trend visualization
* Monthly revenue analysis
* Top countries by revenue
* Top products by revenue
* Product-level details
* 30-day revenue forecast
* Forecast KPIs
* Forecast visualization
* Forecast details table

Run the dashboard using:

```bash
streamlit run dashboard/app.py
```

---

## ⚙️ Pipeline Automation

The complete workflow is automated using:

```text
pipeline/run_pipeline.py
```

It executes:

```text
1. Data Ingestion
2. Data Validation
3. ETL / Transformation
4. PostgreSQL Data Loading
5. Analytics Generation
6. Revenue Forecasting
```

The pipeline provides:

* Sequential execution
* Stage-level status reporting
* Error detection
* Failure handling
* Execution time measurement
* Final pipeline summary

### Verified Pipeline Run

```text
Pipeline stages completed: 6/6
Execution time: ~2 minutes 36 seconds
Database records: 524,878
Total revenue: £10,642,110.80
Total orders: 19,960
```

---

## 🧪 Testing

Automated tests are implemented using **Pytest**.

The test suite verifies:

* Raw dataset availability
* Processed dataset availability
* Dataset non-emptiness
* Required columns
* Positive quantities
* Positive prices
* Revenue calculation
* Forecast file generation
* Exactly 30 forecast days
* Non-negative forecast values
* Analytics output generation

### Test Result

```text
11 passed
```

Run the tests using:

```bash
python -m pytest -v
```

---

## 📁 Project Structure

```text
RetailFlow/
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── output/
│
├── ingestion/
│   └── ingest_data.py
│
├── validation/
│   └── validate_data.py
│
├── etl/
│   └── transform_data.py
│
├── database/
│   └── load_data.py
│
├── analytics/
│   └── generate_analytics.py
│
├── forecasting/
│   └── forecast_revenue.py
│
├── dashboard/
│   └── app.py
│
├── pipeline/
│   └── run_pipeline.py
│
├── tests/
│   └── test_pipeline.py
│
├── .gitignore
├── README.md
├── requirements.txt
└── .env
```

---

## 🛠️ Technology Stack

| Technology    | Purpose                    |
| ------------- | -------------------------- |
| Python        | Core development           |
| Pandas        | Data processing            |
| NumPy         | Numerical operations       |
| OpenPyXL      | Excel dataset handling     |
| PostgreSQL    | Data storage               |
| SQLAlchemy    | Database connectivity      |
| Scikit-learn  | Revenue forecasting        |
| Streamlit     | Dashboard                  |
| Plotly        | Interactive visualizations |
| Pytest        | Automated testing          |
| python-dotenv | Environment configuration  |
| Git & GitHub  | Version control            |

---

## 🚀 Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/RetailFlow.git
cd RetailFlow
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```bash
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file containing your PostgreSQL configuration.

Example:

```text
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retailflow
DB_USER=postgres
DB_PASSWORD=your_password
```

**Do not commit `.env` to GitHub.**

### 5. Add the dataset

Place:

```text
Online Retail.xlsx
```

inside:

```text
data/raw/
```

### 6. Run the complete pipeline

```bash
python pipeline/run_pipeline.py
```

### 7. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

---

## 📝 Important Repository Notes

Large datasets and generated runtime files are excluded from GitHub through `.gitignore`.

The following are intentionally not committed:

```text
.env
venv/
data/raw/
data/processed/
data/output/
```

This keeps the repository lightweight and prevents database credentials or local environment files from being exposed.

---

## ✅ Final Results

RetailFlow successfully demonstrates an end-to-end Data Engineering workflow.

| Category              |                 Result |
| --------------------- | ---------------------: |
| Raw records processed |                541,909 |
| Cleaned records       |                524,878 |
| PostgreSQL records    |                524,878 |
| Total revenue         |         £10,642,110.80 |
| Total orders          |                 19,960 |
| Unique customers      |                  4,338 |
| Average order value   |                £533.17 |
| Forecast horizon      |                30 days |
| Forecast R²           |                 0.3375 |
| Automated tests       |           11/11 passed |
| Pipeline stages       |                      6 |
| Pipeline runtime      |          ~2 min 36 sec |
| Pipeline status       | Successfully completed |

---

## 🔮 Future Scope

Possible future improvements include:

* Apache Airflow-based orchestration
* Real-time data ingestion
* Cloud database deployment
* Data warehouse integration
* Incremental ETL processing
* Automated data quality monitoring
* Advanced forecasting models
* Customer segmentation
* Sales anomaly detection
* Role-based dashboard access
* Docker containerization
* Cloud deployment

Made by Author :- **Roll No. 37 (D12A) — Sahil Mirchandani**

## 📌 Conclusion

RetailFlow demonstrates how raw e-commerce transaction data can be converted into a reliable analytical data platform through a structured Data Engineering pipeline.

The system integrates **data ingestion, validation, ETL, PostgreSQL storage, analytics, machine-learning-based forecasting, dashboard visualization, automation, and testing** into a single workflow.

The project successfully completed the complete automated pipeline with **6 stages and 11/11 automated tests passing**, providing a practical demonstration of an end-to-end Data Engineering system.
