import os
from pathlib import Path

import pandas as pd
import streamlit as st # pyright: ignore[reportMissingImports]
import plotly.express as px # pyright: ignore[reportMissingImports]
from sqlalchemy import create_engine
from dotenv import load_dotenv


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

@st.cache_resource
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
# LOAD TRANSACTION DATA
# --------------------------------------------------

@st.cache_data
def load_data():
    engine = get_engine()

    query = """
    SELECT
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
    FROM sales_transactions;
    """

    df = pd.read_sql(query, engine)

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"]
    )

    return df


# --------------------------------------------------
# LOAD REVENUE FORECAST
# --------------------------------------------------

@st.cache_data
def load_forecast():

    forecast_file = (
        PROJECT_ROOT
        / "data"
        / "output"
        / "revenue_forecast.csv"
    )

    if not forecast_file.exists():
        return None

    forecast = pd.read_csv(forecast_file)

    forecast["date"] = pd.to_datetime(
        forecast["date"]
    )

    return forecast


# --------------------------------------------------
# STREAMLIT PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="RetailFlow",
    page_icon="📊",
    layout="wide"
)


# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.title("📊 RetailFlow")

st.subheader(
    "E-Commerce Sales Data Pipeline Dashboard"
)

st.caption(
    "Analytics dashboard powered by the RetailFlow "
    "Data Engineering pipeline and PostgreSQL database."
)


# --------------------------------------------------
# MAIN APPLICATION
# --------------------------------------------------

try:

    # Load data
    df = load_data()

    # --------------------------------------------------
    # SIDEBAR FILTERS
    # --------------------------------------------------

    st.sidebar.header("Filters")

    min_date = df["transaction_date"].min().date()
    max_date = df["transaction_date"].max().date()

    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    countries = sorted(
        df["country"]
        .dropna()
        .unique()
    )

    selected_countries = st.sidebar.multiselect(
        "Select Countries",
        options=countries,
        default=countries
    )

    # --------------------------------------------------
    # APPLY FILTERS
    # --------------------------------------------------

    filtered_df = df.copy()

    if len(date_range) == 2:

        start_date, end_date = date_range

        filtered_df = filtered_df[
            (
                filtered_df["transaction_date"].dt.date
                >= start_date
            )
            &
            (
                filtered_df["transaction_date"].dt.date
                <= end_date
            )
        ]

    if selected_countries:

        filtered_df = filtered_df[
            filtered_df["country"].isin(
                selected_countries
            )
        ]

    # --------------------------------------------------
    # CALCULATE KPIs
    # --------------------------------------------------

    total_revenue = filtered_df["revenue"].sum()

    total_orders = (
        filtered_df["invoice_no"].nunique()
    )

    total_quantity = (
        filtered_df["quantity"].sum()
    )

    valid_customers = (
        filtered_df[
            filtered_df["customer_id"] != -1
        ]["customer_id"]
        .nunique()
    )

    average_order_value = (
        total_revenue / total_orders
        if total_orders > 0
        else 0
    )

    # --------------------------------------------------
    # BUSINESS OVERVIEW
    # --------------------------------------------------

    st.markdown("## Business Overview")

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "Total Revenue",
        f"£{total_revenue:,.2f}"
    )

    col2.metric(
        "Total Orders",
        f"{total_orders:,}"
    )

    col3.metric(
        "Units Sold",
        f"{int(total_quantity):,}"
    )

    col4.metric(
        "Customers",
        f"{valid_customers:,}"
    )

    col5.metric(
        "Avg. Order Value",
        f"£{average_order_value:,.2f}"
    )

    st.divider()

    # --------------------------------------------------
    # REVENUE TREND
    # --------------------------------------------------

    st.markdown("## Revenue Trend")

    daily_revenue = (
        filtered_df
        .groupby(
            "transaction_date",
            as_index=False
        )["revenue"]
        .sum()
        .sort_values("transaction_date")
    )

    st.line_chart(
        daily_revenue,
        x="transaction_date",
        y="revenue"
    )

    # --------------------------------------------------
    # MONTHLY REVENUE
    # --------------------------------------------------

    st.markdown("## Monthly Revenue")

    monthly_revenue = filtered_df.copy()

    monthly_revenue["month"] = (
        monthly_revenue["transaction_date"]
        .dt.to_period("M")
        .astype(str)
    )

    monthly_revenue = (
        monthly_revenue
        .groupby(
            "month",
            as_index=False
        )["revenue"]
        .sum()
    )

    st.bar_chart(
        monthly_revenue,
        x="month",
        y="revenue"
    )

    st.divider()

    # --------------------------------------------------
    # COUNTRY AND PRODUCT ANALYSIS
    # --------------------------------------------------

    left, right = st.columns(2)

    # Top Countries
    with left:

        st.markdown(
            "## Top Countries by Revenue"
        )

        country_revenue = (
            filtered_df
            .groupby(
                "country",
                as_index=False
            )["revenue"]
            .sum()
            .sort_values(
                "revenue",
                ascending=False
            )
            .head(10)
        )

        st.bar_chart(
            country_revenue,
            x="country",
            y="revenue"
        )

    # Top Products
    with right:

        st.markdown(
            "## Top Products by Revenue"
        )

        top_products = (
            filtered_df
            .groupby(
                ["stock_code", "description"],
                as_index=False
            )
            .agg(
                units_sold=(
                    "quantity",
                    "sum"
                ),
                revenue=(
                    "revenue",
                    "sum"
                )
            )
            .sort_values(
                "revenue",
                ascending=False
            )
            .head(10)
        )

        display_products = (
            top_products[
                ["description", "revenue"]
            ]
        )

        st.bar_chart(
            display_products,
            x="description",
            y="revenue"
        )

    # --------------------------------------------------
    # TOP PRODUCT DETAILS
    # --------------------------------------------------

    st.divider()

    st.markdown(
        "## Top Product Details"
    )

    st.dataframe(
        top_products,
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        f"Displaying analytics from "
        f"{len(filtered_df):,} processed "
        f"transaction records."
    )

    # --------------------------------------------------
    # 30-DAY REVENUE FORECAST
    # --------------------------------------------------

    st.divider()

    st.markdown(
        "## 30-Day Revenue Forecast"
    )

    forecast_df = load_forecast()

    if forecast_df is not None:

        # Forecast KPIs
        forecast_total = (
            forecast_df["predicted_revenue"]
            .sum()
        )

        forecast_average = (
            forecast_df["predicted_revenue"]
            .mean()
        )

        forecast_max = (
            forecast_df["predicted_revenue"]
            .max()
        )

        forecast_min = (
            forecast_df["predicted_revenue"]
            .min()
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Forecasted 30-Day Revenue",
            f"£{forecast_total:,.2f}"
        )

        col2.metric(
            "Average Daily Forecast",
            f"£{forecast_average:,.2f}"
        )

        col3.metric(
            "Highest Daily Forecast",
            f"£{forecast_max:,.2f}"
        )

        col4.metric(
            "Lowest Daily Forecast",
            f"£{forecast_min:,.2f}"
        )

        # --------------------------------------------------
        # FORECAST CHART
        # --------------------------------------------------

        st.markdown(
            "### Predicted Daily Revenue"
        )

        fig = px.line(
            forecast_df,
            x="date",
            y="predicted_revenue",
            markers=True,
            title="Next 30 Days Revenue Forecast"
        )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Predicted Revenue (£)",
            hovermode="x unified"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

        # --------------------------------------------------
        # FORECAST TABLE
        # --------------------------------------------------

        st.markdown(
            "### Forecast Details"
        )

        display_forecast = (
            forecast_df.copy()
        )

        display_forecast[
            "predicted_revenue"
        ] = (
            display_forecast[
                "predicted_revenue"
            ].round(2)
        )

        st.dataframe(
            display_forecast,
            use_container_width=True,
            hide_index=True
        )

        st.caption(
            "Forecast generated using the "
            "RetailFlow Random Forest revenue "
            "forecasting model."
        )

    else:

        st.warning(
            "Forecast data not found. "
            "Run the forecasting module first."
        )


# --------------------------------------------------
# ERROR HANDLING
# --------------------------------------------------

except Exception as error:

    st.error(
        "Unable to load RetailFlow dashboard."
    )

    st.exception(error)