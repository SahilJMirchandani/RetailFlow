import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Add project root so all modules can be imported
sys.path.insert(0, str(PROJECT_ROOT))


def run_stage(stage_name, stage_function):
    """Run one pipeline stage and stop if it fails."""

    print("\n" + "=" * 70)
    print(f"STARTING: {stage_name}")
    print("=" * 70)

    start_time = datetime.now()

    try:
        success = stage_function()

        if not success:
            print(f"\n❌ {stage_name} FAILED")
            return False

        end_time = datetime.now()
        duration = end_time - start_time

        print(f"\n✅ {stage_name} COMPLETED")
        print(f"Duration: {duration}")

        return True

    except Exception as error:
        print(f"\n❌ {stage_name} FAILED")
        print(f"Error: {error}")
        return False


def run_pipeline():
    """Execute the complete RetailFlow data pipeline."""

    pipeline_start = datetime.now()

    print("\n")
    print("=" * 70)
    print("             RETAILFLOW - AUTOMATED DATA PIPELINE")
    print("=" * 70)
    print(f"Pipeline started: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # --------------------------------------------------
    # IMPORT PIPELINE MODULES
    # --------------------------------------------------

    from ingestion.ingest_data import ingest_data
    from validation.validate_data import validate_data
    from etl.transform_data import transform_data
    from database.load_data import load_data
    from analytics.generate_analytics import run_analytics
    from forecasting.forecast_revenue import train_model, generate_forecast, save_forecast

    # --------------------------------------------------
    # PHASE 1 - DATA INGESTION
    # --------------------------------------------------

    if not run_stage(
        "PHASE 1 - DATA INGESTION",
        ingest_data
    ):
        return False

    # --------------------------------------------------
    # PHASE 2 - DATA VALIDATION
    # --------------------------------------------------

    if not run_stage(
        "PHASE 2 - DATA VALIDATION",
        validate_data
    ):
        return False

    # --------------------------------------------------
    # PHASE 3 - ETL / TRANSFORMATION
    # --------------------------------------------------

    if not run_stage(
        "PHASE 3 - ETL / TRANSFORMATION",
        transform_data
    ):
        return False

    # --------------------------------------------------
    # PHASE 4 - POSTGRESQL DATA LOADING
    # --------------------------------------------------

    if not run_stage(
        "PHASE 4 - POSTGRESQL DATA LOADING",
        load_data
    ):
        return False

    # --------------------------------------------------
    # PHASE 5 - ANALYTICS
    # --------------------------------------------------

    if not run_stage(
        "PHASE 5 - ANALYTICS",
        run_analytics
    ):
        return False

    # --------------------------------------------------
    # PHASE 6 - REVENUE FORECASTING
    # --------------------------------------------------

    def forecasting_stage():

        model, feature_df, feature_columns = train_model()

        forecast_df = generate_forecast(
            model,
            feature_df,
            feature_columns,
            days=30
        )

        save_forecast(forecast_df)

        return True

    if not run_stage(
        "PHASE 6 - REVENUE FORECASTING",
        forecasting_stage
    ):
        return False

    # --------------------------------------------------
    # PIPELINE COMPLETED
    # --------------------------------------------------

    pipeline_end = datetime.now()
    total_duration = pipeline_end - pipeline_start

    print("\n")
    print("=" * 70)
    print("             RETAILFLOW PIPELINE COMPLETED")
    print("=" * 70)

    print(
        f"Started : "
        f"{pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(
        f"Finished: "
        f"{pipeline_end.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(f"Duration: {total_duration}")

    print("\nPipeline stages completed:")
    print("  ✅ Data Ingestion")
    print("  ✅ Data Validation")
    print("  ✅ ETL / Transformation")
    print("  ✅ PostgreSQL Data Loading")
    print("  ✅ Analytics")
    print("  ✅ Revenue Forecasting")

    print("\nRetailFlow data pipeline executed successfully!")

    return True


if __name__ == "__main__":

    success = run_pipeline()

    if not success:
        print("\n❌ RetailFlow pipeline execution failed.")
        raise SystemExit(1)