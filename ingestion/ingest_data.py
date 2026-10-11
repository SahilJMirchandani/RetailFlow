"""Ingest the original Online Retail workbook into the project data folder.

Source resolution order:
1. An explicit path passed to ingest_data(source_file=...).
2. RETAILFLOW_ONLINE_RETAIL_SOURCE environment variable.
3. The canonical data/raw/Online Retail.xlsx file, if it already exists.
4. The legacy Desktop/ML_Self_Learning location for backward compatibility.
"""

import os
import shutil
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_FILE = RAW_DIR / "Online Retail.xlsx"
LEGACY_SOURCE_FILE = (
    Path.home()
    / "Desktop"
    / "ML_Self_Learning"
    / "data"
    / "Online Retail.xlsx"
)
SOURCE_ENV_VAR = "RETAILFLOW_ONLINE_RETAIL_SOURCE"


def resolve_source_file(source_file=None):
    """Resolve an available source workbook without assuming a user-specific path."""

    if source_file is not None:
        candidates = [Path(source_file).expanduser()]
    else:
        configured_source = os.getenv(SOURCE_ENV_VAR)
        if configured_source:
            # Honor explicit configuration; report an error if it is incorrect
            # instead of silently falling back to a different dataset.
            candidates = [Path(configured_source).expanduser()]
        else:
            candidates = [RAW_FILE, LEGACY_SOURCE_FILE]

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    return None


def ingest_data(source_file=None):
    """Copy the source workbook into data/raw unless it is already there."""

    print("=" * 60)
    print("RetailFlow - Data Ingestion")
    print("=" * 60)

    source = resolve_source_file(source_file)

    if source is None:
        print("ERROR: Online Retail dataset not found.")
        print(f"Place it at: {RAW_FILE}")
        print(
            f"Or set {SOURCE_ENV_VAR} to the full path of "
            "Online Retail.xlsx."
        )
        return False

    try:
        RAW_DIR.mkdir(parents=True, exist_ok=True)

        # The canonical raw file is already in place; avoid copying a file
        # onto itself (which raises SameFileError).
        if source == RAW_FILE.resolve():
            print(f"Raw file    : {RAW_FILE}")
            print("Dataset is already in the project's raw data directory.")
        else:
            shutil.copy2(source, RAW_FILE)
            print(f"Source file : {source}")
            print(f"Raw file    : {RAW_FILE}")

        file_size_mb = RAW_FILE.stat().st_size / (1024 * 1024)
        ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"File size   : {file_size_mb:.2f} MB")
        print(f"Ingested at : {ingestion_time}")
        print("\nData ingestion successful!")
        return True

    except (OSError, shutil.Error) as error:
        print(f"\nERROR during data ingestion: {error}")
        return False


if __name__ == "__main__":
    if not ingest_data():
        raise SystemExit(1)
