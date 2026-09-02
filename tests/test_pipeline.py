from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_raw_dataset_exists():
    """Check that the raw input dataset exists."""
    raw_file = PROJECT_ROOT / "data" / "raw" / "Online Retail.xlsx"

    assert raw_file.exists(), "Raw dataset is missing."


def test_processed_dataset_exists():
    """Check that ETL generated the cleaned dataset."""
    processed_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "cleaned_transactions.csv"
    )

    assert processed_file.exists(), "Cleaned dataset is missing."


def test_processed_dataset_not_empty():
    """Check that the cleaned dataset contains records."""
    processed_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "cleaned_transactions.csv"
    )

    df = pd.read_csv(processed_file)

    assert len(df) > 0, "Processed dataset is empty."


def test_processed_dataset_has_required_columns():
    """Check that the ETL output has the required columns."""
    processed_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "cleaned_transactions.csv"
    )

    df = pd.read_csv(processed_file, nrows=5)

    required_columns = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
        "Revenue",
        "TransactionDate"
    ]

    for column in required_columns:
        assert column in df.columns, (
            f"Required column missing: {column}"
        )


def test_processed_data_has_valid_quantity():
    """Check that all quantities are positive."""
    processed_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "cleaned_transactions.csv"
    )

    df = pd.read_csv(
        processed_file,
        usecols=["Quantity"]
    )

    assert (df["Quantity"] > 0).all(), (
        "Processed data contains invalid quantities."
    )


def test_processed_data_has_valid_prices():
    """Check that all unit prices are positive."""
    processed_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "cleaned_transactions.csv"
    )

    df = pd.read_csv(
        processed_file,
        usecols=["UnitPrice"]
    )

    assert (df["UnitPrice"] > 0).all(), (
        "Processed data contains invalid prices."
    )


def test_revenue_calculation():
    """Check that revenue equals quantity multiplied by unit price."""
    processed_file = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "cleaned_transactions.csv"
    )

    df = pd.read_csv(
        processed_file,
        usecols=["Quantity", "UnitPrice", "Revenue"]
    )

    calculated_revenue = (
        df["Quantity"] * df["UnitPrice"]
    )

    assert (
        (df["Revenue"] - calculated_revenue).abs() < 0.01
    ).all(), "Revenue calculation is incorrect."


def test_forecast_file_exists():
    """Check that the forecasting stage generated its output."""
    forecast_file = (
        PROJECT_ROOT
        / "data"
        / "output"
        / "revenue_forecast.csv"
    )

    assert forecast_file.exists(), (
        "Revenue forecast file is missing."
    )


def test_forecast_has_30_days():
    """Check that the forecast contains 30 future predictions."""
    forecast_file = (
        PROJECT_ROOT
        / "data"
        / "output"
        / "revenue_forecast.csv"
    )

    df = pd.read_csv(forecast_file)

    assert len(df) == 30, (
        f"Expected 30 forecast records, found {len(df)}."
    )


def test_forecast_values_are_valid():
    """Check that forecast values are non-negative."""
    forecast_file = (
        PROJECT_ROOT
        / "data"
        / "output"
        / "revenue_forecast.csv"
    )

    df = pd.read_csv(forecast_file)

    assert (
        df["predicted_revenue"] >= 0
    ).all(), "Forecast contains negative revenue values."


def test_analytics_outputs_exist():
    """Check that analytics output files were generated."""
    expected_files = [
        "revenue_by_country.csv",
        "top_products.csv",
        "daily_revenue.csv",
        "monthly_revenue.csv"
    ]

    for filename in expected_files:
        output_file = (
            PROJECT_ROOT
            / "data"
            / "output"
            / filename
        )

        assert output_file.exists(), (
            f"Analytics output missing: {filename}"
        )