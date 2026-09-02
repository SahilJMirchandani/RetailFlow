import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

load_dotenv(PROJECT_ROOT / ".env")


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

def get_engine():

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    connection_string = (
        f"postgresql+psycopg://{user}:{password}@"
        f"{host}:{port}/{database}"
    )

    return create_engine(connection_string)


# --------------------------------------------------
# ANALYTICS
# --------------------------------------------------

def run_analytics():

    print("=" * 60)
    print("RetailFlow - Analytics")
    print("=" * 60)

    engine = None

    try:

        print("\n[1] Connecting to PostgreSQL...")

        engine = get_engine()

        OUTPUT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        # --------------------------------------------------
        # OVERALL KPIs
        # --------------------------------------------------

        print("[2] Calculating overall KPIs...")

        kpi_query = """
        SELECT
            COUNT(*) AS total_records,
            SUM(quantity) AS total_quantity,
            SUM(revenue) AS total_revenue,

            COUNT(DISTINCT customer_id)
                FILTER (
                    WHERE customer_id != -1
                ) AS unique_customers,

            COUNT(DISTINCT invoice_no)
                AS total_orders

        FROM sales_transactions;
        """

        kpis = pd.read_sql(
            kpi_query,
            engine
        )

        print("\nOverall KPIs")
        print("-" * 40)

        print(
            kpis.to_string(
                index=False
            )
        )

        # --------------------------------------------------
        # REVENUE BY COUNTRY
        # --------------------------------------------------

        print(
            "\n[3] Calculating revenue by country..."
        )

        country_query = """
        SELECT
            country,
            ROUND(
                SUM(revenue),
                2
            ) AS total_revenue

        FROM sales_transactions

        GROUP BY country

        ORDER BY total_revenue DESC;
        """

        revenue_by_country = pd.read_sql(
            country_query,
            engine
        )

        revenue_by_country.to_csv(
            OUTPUT_DIR / "revenue_by_country.csv",
            index=False
        )

        # --------------------------------------------------
        # TOP PRODUCTS
        # --------------------------------------------------

        print(
            "[4] Calculating top products..."
        )

        product_query = """
        SELECT
            stock_code,
            description,
            SUM(quantity) AS units_sold,

            ROUND(
                SUM(revenue),
                2
            ) AS total_revenue

        FROM sales_transactions

        GROUP BY
            stock_code,
            description

        ORDER BY
            total_revenue DESC

        LIMIT 20;
        """

        top_products = pd.read_sql(
            product_query,
            engine
        )

        top_products.to_csv(
            OUTPUT_DIR / "top_products.csv",
            index=False
        )

        # --------------------------------------------------
        # DAILY REVENUE
        # --------------------------------------------------

        print(
            "[5] Calculating daily revenue..."
        )

        daily_query = """
        SELECT
            transaction_date,
            ROUND(
                SUM(revenue),
                2
            ) AS daily_revenue

        FROM sales_transactions

        GROUP BY transaction_date

        ORDER BY transaction_date;
        """

        daily_revenue = pd.read_sql(
            daily_query,
            engine
        )

        daily_revenue.to_csv(
            OUTPUT_DIR / "daily_revenue.csv",
            index=False
        )

        # --------------------------------------------------
        # MONTHLY REVENUE
        # --------------------------------------------------

        print(
            "[6] Calculating monthly revenue..."
        )

        monthly_query = """
        SELECT
            DATE_TRUNC(
                'month',
                transaction_date
            )::date AS month,

            ROUND(
                SUM(revenue),
                2
            ) AS monthly_revenue

        FROM sales_transactions

        GROUP BY month

        ORDER BY month;
        """

        monthly_revenue = pd.read_sql(
            monthly_query,
            engine
        )

        monthly_revenue.to_csv(
            OUTPUT_DIR / "monthly_revenue.csv",
            index=False
        )

        # --------------------------------------------------
        # AVERAGE ORDER VALUE
        # --------------------------------------------------

        print(
            "[7] Calculating average order value..."
        )

        order_query = """
        SELECT
            ROUND(
                AVG(order_total),
                2
            ) AS average_order_value

        FROM (

            SELECT
                invoice_no,
                SUM(revenue) AS order_total

            FROM sales_transactions

            GROUP BY invoice_no

        ) AS orders;
        """

        average_order = pd.read_sql(
            order_query,
            engine
        )

        print("\nAverage Order Value")
        print("-" * 40)

        print(
            average_order.to_string(
                index=False
            )
        )

        # --------------------------------------------------
        # COMPLETION
        # --------------------------------------------------

        print("\nAnalytics completed successfully!")

        print("-" * 40)

        print("Generated:")

        print(
            "  - revenue_by_country.csv"
        )

        print(
            "  - top_products.csv"
        )

        print(
            "  - daily_revenue.csv"
        )

        print(
            "  - monthly_revenue.csv"
        )

        return True

    except Exception as error:

        print(
            f"\nERROR during analytics: {error}"
        )

        return False

    finally:

        if engine is not None:
            engine.dispose()


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    success = run_analytics()

    if not success:
        raise SystemExit(1)