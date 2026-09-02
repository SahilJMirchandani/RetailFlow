import os
from pathlib import Path

import pandas as pd
import psycopg # pyright: ignore[reportMissingImports]
from dotenv import load_dotenv


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "cleaned_transactions.csv"
)

load_dotenv(PROJECT_ROOT / ".env")


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

def get_connection():

    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


# --------------------------------------------------
# CREATE TABLE
# --------------------------------------------------

def create_table(connection):

    query = """
    CREATE TABLE IF NOT EXISTS sales_transactions (

        transaction_id SERIAL PRIMARY KEY,

        invoice_no VARCHAR(20),

        stock_code VARCHAR(20),

        description TEXT,

        quantity INTEGER,

        invoice_date TIMESTAMP,

        unit_price NUMERIC(12, 2),

        customer_id NUMERIC,

        country VARCHAR(100),

        revenue NUMERIC(14, 2),

        transaction_date DATE
    );
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

    connection.commit()


# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

def load_data():

    print("=" * 60)
    print("RetailFlow - PostgreSQL Data Loading")
    print("=" * 60)

    if not DATA_FILE.exists():

        print(
            f"ERROR: Processed dataset not found: "
            f"{DATA_FILE}"
        )

        return False

    connection = None

    try:

        # --------------------------------------------------
        # READ CLEANED DATA
        # --------------------------------------------------

        print("\n[1] Reading cleaned dataset...")

        df = pd.read_csv(
            DATA_FILE,
            dtype={
                "InvoiceNo": str,
                "StockCode": str
            }
        )

        print(
            f"Records to load: {len(df)}"
        )

        # --------------------------------------------------
        # CONNECT TO DATABASE
        # --------------------------------------------------

        print("\n[2] Connecting to PostgreSQL...")

        connection = get_connection()

        # --------------------------------------------------
        # CREATE TABLE
        # --------------------------------------------------

        print(
            "[3] Creating sales_transactions table..."
        )

        create_table(connection)

        # --------------------------------------------------
        # CLEAR PREVIOUS DATA
        # --------------------------------------------------

        print(
            "[4] Clearing previous transaction data..."
        )

        with connection.cursor() as cursor:

            cursor.execute(
                """
                TRUNCATE TABLE
                sales_transactions
                RESTART IDENTITY;
                """
            )

        connection.commit()

        # --------------------------------------------------
        # PREPARE DATA
        # --------------------------------------------------

        print(
            "[5] Preparing records for database loading..."
        )

        records = df[
            [
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
        ].values.tolist()

        # --------------------------------------------------
        # INSERT DATA
        # --------------------------------------------------

        print(
            "[6] Loading data into PostgreSQL..."
        )

        insert_query = """
        INSERT INTO sales_transactions (

            invoice_no,
            stock_code,
            description,
            quantity,
            invoice_date,
            unit_price,
            customer_id,
            country,
            revenue,
            transaction_date

        )

        VALUES (

            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s

        );
        """

        with connection.cursor() as cursor:

            cursor.executemany(
                insert_query,
                records
            )

        connection.commit()

        # --------------------------------------------------
        # VERIFY RECORD COUNT
        # --------------------------------------------------

        print(
            "[7] Verifying database record count..."
        )

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM sales_transactions;
                """
            )

            database_count = cursor.fetchone()[0]

        print("\nData loading successful!")
        print("-" * 40)

        print(
            f"Records loaded       : {len(records)}"
        )

        print(
            f"Database record count : {database_count}"
        )

        print(
            "Table created        : sales_transactions"
        )

        # --------------------------------------------------
        # FINAL VALIDATION
        # --------------------------------------------------

        if database_count != len(records):

            print(
                "\nERROR: Database record count "
                "does not match source records."
            )

            return False

        print(
            "\nDatabase verification successful!"
        )

        return True

    except Exception as error:

        if connection is not None:
            connection.rollback()

        print(
            f"\nERROR during database loading: {error}"
        )

        return False

    finally:

        if connection is not None:
            connection.close()


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    success = load_data()

    if not success:
        raise SystemExit(1)