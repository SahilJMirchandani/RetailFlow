import shutil
from pathlib import Path
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = (
    Path.home()
    / "Desktop"
    / "ML_Self_Learning"
    / "data"
    / "Online Retail.xlsx"
)

RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_FILE = RAW_DIR / "Online Retail.xlsx"


def ingest_data():
    print("=" * 60)
    print("RetailFlow - Data Ingestion")
    print("=" * 60)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        print("ERROR: Source dataset not found.")
        print(f"Expected location: {SOURCE_FILE}")
        return False

    try:
        shutil.copy2(SOURCE_FILE, RAW_FILE)

        file_size_mb = RAW_FILE.stat().st_size / (1024 * 1024)
        ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"Source file : {SOURCE_FILE}")
        print(f"Raw file    : {RAW_FILE}")
        print(f"File size   : {file_size_mb:.2f} MB")
        print(f"Ingested at : {ingestion_time}")

        print("\nData ingestion successful!")
        return True

    except Exception as error:
        print(f"\nERROR during data ingestion: {error}")
        return False


if __name__ == "__main__":
    success = ingest_data()

    if not success:
        raise SystemExit(1)