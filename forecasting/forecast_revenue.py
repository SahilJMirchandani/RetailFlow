import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

load_dotenv(PROJECT_ROOT / ".env")


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


def load_daily_revenue():
    engine = get_engine()

    query = """
    SELECT
        transaction_date,
        SUM(revenue) AS daily_revenue
    FROM sales_transactions
    GROUP BY transaction_date
    ORDER BY transaction_date;
    """

    df = pd.read_sql(query, engine)

    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["daily_revenue"] = pd.to_numeric(df["daily_revenue"])

    return df


def prepare_features(df):
    df = df.copy()

    df = df.set_index("transaction_date")

    # Create continuous daily dates
    full_dates = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq="D"
    )

    df = df.reindex(full_dates)
    df["daily_revenue"] = df["daily_revenue"].fillna(0)

    df.index.name = "transaction_date"

    # Calendar features
    df["day_of_week"] = df.index.dayofweek
    df["day_of_month"] = df.index.day
    df["month"] = df.index.month
    df["week_of_year"] = df.index.isocalendar().week.astype(int)

    # Lag features
    df["lag_1"] = df["daily_revenue"].shift(1)
    df["lag_7"] = df["daily_revenue"].shift(7)
    df["lag_14"] = df["daily_revenue"].shift(14)

    # Rolling average using only previous days
    df["rolling_7_day_mean"] = (
        df["daily_revenue"]
        .shift(1)
        .rolling(7)
        .mean()
    )

    df = df.dropna()

    return df


def train_model():
    print("=" * 60)
    print("RetailFlow - Revenue Forecasting")
    print("=" * 60)

    print("\n[1] Loading daily revenue from PostgreSQL...")

    daily_df = load_daily_revenue()

    print(f"Original daily records: {len(daily_df)}")

    print("\n[2] Creating forecasting features...")

    feature_df = prepare_features(daily_df)

    print(f"Feature records: {len(feature_df)}")

    feature_columns = [
        "day_of_week",
        "day_of_month",
        "month",
        "week_of_year",
        "lag_1",
        "lag_7",
        "lag_14",
        "rolling_7_day_mean"
    ]

    X = feature_df[feature_columns]
    y = feature_df["daily_revenue"]

    # Chronological train-test split
    split_index = int(len(feature_df) * 0.8)

    X_train = X.iloc[:split_index]
    X_test = X.iloc[split_index:]

    y_train = y.iloc[:split_index]
    y_test = y.iloc[split_index:]

    print("\n[3] Training Random Forest model...")

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_leaf=4,
        min_samples_split=2,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    r2 = r2_score(y_test, predictions)

    print("\nModel Evaluation")
    print("-" * 40)
    print(f"MAE  : {mae:,.2f}")
    print(f"RMSE : {rmse:,.2f}")
    print(f"R²   : {r2:.4f}")

    # Retrain using all historical data
    print("\n[4] Retraining model on complete historical data...")

    final_model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_leaf=4,
        min_samples_split=2,
        random_state=42,
        n_jobs=-1
    )

    final_model.fit(X, y)

    return final_model, feature_df, feature_columns


def generate_forecast(model, history_df, feature_columns, days=30):
    print(f"\n[5] Generating {days}-day revenue forecast...")

    history = history_df["daily_revenue"].copy()

    future_dates = pd.date_range(
        start=history.index.max() + pd.Timedelta(days=1),
        periods=days,
        freq="D"
    )

    forecasts = []

    for future_date in future_dates:

        row = {
            "day_of_week": future_date.dayofweek,
            "day_of_month": future_date.day,
            "month": future_date.month,
            "week_of_year": int(future_date.isocalendar().week),
            "lag_1": history.iloc[-1],
            "lag_7": history.iloc[-7],
            "lag_14": history.iloc[-14],
            "rolling_7_day_mean": history.iloc[-7:].mean()
        }

        X_future = pd.DataFrame([row])[feature_columns]

        prediction = model.predict(X_future)[0]

        # Revenue cannot be negative
        prediction = max(0, prediction)

        forecasts.append({
            "date": future_date,
            "predicted_revenue": prediction
        })

        history.loc[future_date] = prediction

    forecast_df = pd.DataFrame(forecasts)

    return forecast_df


def save_forecast(forecast_df):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_DIR / "revenue_forecast.csv"

    forecast_df.to_csv(output_file, index=False)

    print("\nForecast saved successfully!")
    print(f"Output file: {output_file}")

    return output_file


if __name__ == "__main__":

    try:
        model, feature_df, feature_columns = train_model()

        forecast_df = generate_forecast(
            model,
            feature_df,
            feature_columns,
            days=30
        )

        save_forecast(forecast_df)

        print("\nForecast Preview")
        print("-" * 40)
        print(forecast_df.head(10).to_string(index=False))

        print("\nRevenue forecasting completed successfully!")

    except Exception as error:
        print(f"\nERROR during forecasting: {error}")
        raise