import pandas as pd
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_FILE = PROJECT_ROOT / "data" / "raw" / "Online Retail.xlsx"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

CLEANED_FILE = PROCESSED_DIR / "cleaned_transactions.csv"
DAILY_FILE = PROCESSED_DIR / "daily_revenue.csv"


def transform_data():
    print("=" * 60)
    print("RetailFlow - ETL / Data Transformation")
    print("=" * 60)

    if not RAW_FILE.exists():
        print(f"ERROR: Raw dataset not found: {RAW_FILE}")
        return False

    try:
        print("\n[1] Reading raw dataset...")
        df = pd.read_excel(RAW_FILE)
        print(f"Initial records: {len(df)}")

        # Remove duplicate records
        before = len(df)
        df = df.drop_duplicates()
        print(f"[2] Removed duplicates: {before - len(df)}")

        # Remove cancelled transactions
        before = len(df)
        df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]
        print(f"[3] Removed cancelled transactions: {before - len(df)}")

        # Remove invalid quantity and price values
        before = len(df)
        df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]
        print(f"[4] Removed invalid quantity/price records: {before - len(df)}")

        # Convert date column
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

        # Handle missing customer IDs
        df["CustomerID"] = df["CustomerID"].fillna(-1)

        # Calculate revenue
        df["Revenue"] = df["Quantity"] * df["UnitPrice"]

        # Extract transaction date
        df["TransactionDate"] = df["InvoiceDate"].dt.date

        # Select cleaned transaction columns
        cleaned_columns = [
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

        cleaned_df = df[cleaned_columns].copy()

        # Create daily revenue dataset
        daily_revenue = (
            cleaned_df
            .groupby("TransactionDate", as_index=False)["Revenue"]
            .sum()
        )

        daily_revenue["TransactionDate"] = pd.to_datetime(
            daily_revenue["TransactionDate"]
        )

        daily_revenue = daily_revenue.sort_values("TransactionDate")

        # Save processed datasets
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        cleaned_df.to_csv(CLEANED_FILE, index=False)
        daily_revenue.to_csv(DAILY_FILE, index=False)

        print("\nETL completed successfully!")
        print("-" * 40)
        print(f"Cleaned transactions : {len(cleaned_df)}")
        print(f"Daily revenue rows   : {len(daily_revenue)}")
        print(f"Cleaned data saved   : {CLEANED_FILE}")
        print(f"Daily data saved     : {DAILY_FILE}")

        return True

    except Exception as error:
        print(f"\nERROR during ETL: {error}")
        return False


if __name__ == "__main__":
    success = transform_data()

    if not success:
        raise SystemExit(1)