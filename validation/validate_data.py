import pandas as pd
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "Online Retail.xlsx"
REPORT_FILE = PROJECT_ROOT / "data" / "output" / "validation_report.txt"

EXPECTED_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country"
]


def validate_data():
    print("=" * 60)
    print("RetailFlow - Data Validation")
    print("=" * 60)

    if not RAW_FILE.exists():
        print(f"ERROR: Raw dataset not found: {RAW_FILE}")
        return False

    try:
        df = pd.read_excel(RAW_FILE)

        print(f"\nDataset shape: {df.shape}")

        # Schema validation
        missing_columns = [
            column for column in EXPECTED_COLUMNS
            if column not in df.columns
        ]

        schema_valid = len(missing_columns) == 0

        # Missing values
        missing_values = df.isnull().sum()
        total_missing = missing_values.sum()

        # Duplicate records
        duplicate_count = df.duplicated().sum()

        # Invalid numerical values
        invalid_quantity = (df["Quantity"] <= 0).sum()
        invalid_price = (df["UnitPrice"] <= 0).sum()

        # Date validation
        df["InvoiceDate"] = pd.to_datetime(
            df["InvoiceDate"],
            errors="coerce"
        )

        invalid_dates = df["InvoiceDate"].isnull().sum()

        # Cancellation check
        cancelled_transactions = (
            df["InvoiceNo"]
            .astype(str)
            .str.startswith("C")
            .sum()
        )

        # Validation report
        report = [
            "RetailFlow - Data Validation Report",
            "=" * 50,
            f"Dataset rows              : {len(df)}",
            f"Dataset columns           : {len(df.columns)}",
            "",
            f"Schema valid              : {schema_valid}",
            f"Missing columns           : {missing_columns}",
            f"Total missing values     : {total_missing}",
            f"Duplicate records         : {duplicate_count}",
            f"Invalid quantity records  : {invalid_quantity}",
            f"Invalid price records     : {invalid_price}",
            f"Invalid date records     : {invalid_dates}",
            f"Cancelled transactions    : {cancelled_transactions}",
            "",
            "Validation completed."
        ]

        REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        REPORT_FILE.write_text("\n".join(report), encoding="utf-8")

        print("\nValidation Results")
        print("-" * 40)
        print(f"Schema valid             : {schema_valid}")
        print(f"Total missing values     : {total_missing}")
        print(f"Duplicate records        : {duplicate_count}")
        print(f"Invalid quantity records : {invalid_quantity}")
        print(f"Invalid price records    : {invalid_price}")
        print(f"Invalid date records     : {invalid_dates}")
        print(f"Cancelled transactions   : {cancelled_transactions}")

        print(f"\nValidation report saved to:")
        print(REPORT_FILE)

        return True

    except Exception as error:
        print(f"\nERROR during validation: {error}")
        return False


if __name__ == "__main__":
    success = validate_data()

    if not success:
        raise SystemExit(1)