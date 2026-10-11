
import csv
import json
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
LOG_DIR = OUTPUT_DIR / "logs"
LINEAGE_DIR = OUTPUT_DIR / "lineage"

LATEST_RUN_FILE = LINEAGE_DIR / "pipeline_run_latest.json"
RUN_HISTORY_FILE = LINEAGE_DIR / "pipeline_runs.jsonl"

STAGES = [
    {
        "name": "Product Catalogue API Ingestion",
        "script": "ingestion/ingest_product_catalog.py",
        "inputs": ["DummyJSON Products API"],
        "outputs": [
            "data/bronze/dummyjson/",
            "data/silver/products/products.parquet",
            "data/silver/products/products.csv",
            "data/output/product_catalog_quality.json",
        ],
    },
    {
        "name": "Product Image Ingestion",
        "script": "ingestion/download_product_images.py",
        "inputs": [
            "data/silver/products/products.parquet",
        ],
        "outputs": [
            "data/catalog_images/dummyjson/",
            "data/output/product_images_manifest.csv",
        ],
    },
    {
        "name": "Olist Bronze Ingestion",
        "script": "ingestion/ingest_olist.py",
        "inputs": ["data/raw/olist/*.csv"],
        "outputs": [
            "data/bronze/olist/",
            "data/output/olist_bronze_manifest.json",
        ],
    },
    {
        "name": "Olist Silver Transformation",
        "script": "etl/transform_olist_silver.py",
        "inputs": ["data/bronze/olist/*.parquet"],
        "outputs": [
            "data/silver/olist/",
            "data/output/olist_silver_quality.json",
        ],
    },
    {
        "name": "Olist Gold Warehouse",
        "script": "etl/build_olist_gold.py",
        "inputs": ["data/silver/olist/*.parquet"],
        "outputs": [
            "data/gold/olist/",
            "data/output/olist_gold_quality.json",
        ],
    },
]


def utc_now():
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc).isoformat()


def load_json(relative_path):
    """Load a project JSON file when it exists."""

    path = PROJECT_ROOT / relative_path

    if not path.is_file():
        return None

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return None


def collect_metrics(successful_stages=None):
    """Collect metrics only for stages confirmed successful in this run.

    When successful_stages is provided, reports from stages that did not
    complete successfully are excluded. This prevents a failed run from
    accidentally displaying quality reports left over from an older run.
    Omitting the argument preserves the ability to inspect all existing
    reports outside an orchestrated run.
    """

    successful = (
        None if successful_stages is None else set(successful_stages)
    )

    def stage_succeeded(stage_name):
        return successful is None or stage_name in successful

    metrics = {}

    if stage_succeeded("Product Catalogue API Ingestion"):
        product_report = load_json(
            "data/output/product_catalog_quality.json"
        )

        if product_report:
            metrics["product_catalog"] = {
                "records": product_report.get("records_received"),
                "categories": product_report.get("distinct_categories"),
                "quality_passed": product_report.get("passed"),
            }

    if stage_succeeded("Olist Bronze Ingestion"):
        bronze_report = load_json(
            "data/output/olist_bronze_manifest.json"
        )

        if bronze_report:
            tables = bronze_report.get("tables", [])

            metrics["olist_bronze"] = {
                "tables_succeeded": bronze_report.get("tables_succeeded"),
                "tables_failed": bronze_report.get("tables_failed"),
                "total_rows": sum(
                    table.get("row_count", 0)
                    for table in tables
                ),
                "table_counts": {
                    table["source_file"]: table.get("row_count", 0)
                    for table in tables
                },
            }

    if stage_succeeded("Olist Silver Transformation"):
        silver_report = load_json(
            "data/output/olist_silver_quality.json"
        )

        if silver_report:
            metrics["olist_silver"] = {
                "quality_passed": silver_report.get("passed"),
                "row_counts": silver_report.get("row_counts", {}),
                "checks": silver_report.get("checks", {}),
            }

    if stage_succeeded("Olist Gold Warehouse"):
        gold_report = load_json(
            "data/output/olist_gold_quality.json"
        )

        if gold_report:
            metrics["olist_gold"] = {
                "quality_passed": gold_report.get("passed"),
                "row_counts": gold_report.get("row_counts", {}),
                "checks": gold_report.get("checks", {}),
                "reconciliation": gold_report.get(
                    "reconciliation", {}
                ),
            }

    if stage_succeeded("Product Image Ingestion"):
        manifest_path = (
            OUTPUT_DIR / "product_images_manifest.csv"
        )

        if manifest_path.is_file():
            status_counts = Counter()
            unique_urls = set()
            local_image_count = 0

            with manifest_path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as file:
                for row in csv.DictReader(file):
                    status_counts[
                        row.get("download_status", "unknown")
                    ] += 1

                    url = row.get("image_url")
                    if url:
                        unique_urls.add(url)

                    local_path = row.get("local_path", "")
                    if local_path:
                        absolute_path = PROJECT_ROOT / Path(local_path)

                        if absolute_path.is_file():
                            local_image_count += 1

            metrics["product_images"] = {
                "associations": sum(status_counts.values()),
                "unique_urls": len(unique_urls),
                "local_files_available": local_image_count,
                "download_status_counts": dict(status_counts),
            }

    return metrics

def execute_stage(stage, log_file):
    """Run one component and stream its output to the terminal and log."""

    script_path = PROJECT_ROOT / stage["script"]

    if not script_path.is_file():
        return {
            "name": stage["name"],
            "script": stage["script"],
            "status": "failed",
            "error": f"Script not found: {script_path}",
        }

    print("\n" + "=" * 75)
    print(f"STARTING: {stage['name']}")
    print("=" * 75)

    started_at = utc_now()
    start_clock = time.perf_counter()

    with log_file.open(
        "a",
        encoding="utf-8",
        newline="",
    ) as log:
        log.write(
            f"\n\n[{started_at}] STARTING {stage['name']}\n"
        )
        log.flush()

        try:
            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )

            if process.stdout is not None:
                for line in process.stdout:
                    print(line, end="")
                    log.write(line)
                    log.flush()

            exit_code = process.wait()

        except OSError as error:
            exit_code = 1
            log.write(f"\nProcess launch error: {error}\n")
            print(f"Process launch error: {error}")

    duration = round(time.perf_counter() - start_clock, 3)
    finished_at = utc_now()
    success = exit_code == 0

    result = {
        "name": stage["name"],
        "script": stage["script"],
        "status": "success" if success else "failed",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration,
        "exit_code": exit_code,
        "inputs": stage["inputs"],
        "outputs": stage["outputs"],
    }

    print(
        f"\n{'SUCCESS' if success else 'FAILED'}: "
        f"{stage['name']} ({duration:.2f} seconds)"
    )

    return result


def save_run_record(record):
    """Persist the latest run and append immutable run history."""

    LINEAGE_DIR.mkdir(parents=True, exist_ok=True)

    temporary_file = LATEST_RUN_FILE.with_suffix(".tmp")

    temporary_file.write_text(
        json.dumps(record, indent=2),
        encoding="utf-8",
    )

    temporary_file.replace(LATEST_RUN_FILE)

    with RUN_HISTORY_FILE.open(
        "a",
        encoding="utf-8",
    ) as history:
        history.write(
            json.dumps(record, separators=(",", ":")) + "\n"
        )


def run_retailflow_v2():
    """Run the new catalogue and Olist pipelines in sequence."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LINEAGE_DIR.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    started_at = utc_now()

    log_file = LOG_DIR / f"retailflow_v2_{run_id}.log"

    record = {
        "project": "RetailFlow 2.0",
        "run_id": run_id,
        "status": "running",
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "duration_seconds": None,
        "stage_count": len(STAGES),
        "stages": [],
        "metrics": {},
        "lineage": {
            "history_file": RUN_HISTORY_FILE.relative_to(
                PROJECT_ROOT
            ).as_posix(),
            "latest_run_file": LATEST_RUN_FILE.relative_to(
                PROJECT_ROOT
            ).as_posix(),
            "log_file": log_file.relative_to(
                PROJECT_ROOT
            ).as_posix(),
        },
    }

    print("=" * 75)
    print("RETAILFLOW 2.0 - ORCHESTRATED DATA ENGINEERING PIPELINE")
    print("=" * 75)
    print(f"Run ID : {run_id}")
    print(f"Started: {started_at}")
    print(f"Stages : {len(STAGES)}")
    print("")

    try:
        for stage in STAGES:
            result = execute_stage(stage, log_file)
            record["stages"].append(result)

            if result["status"] != "success":
                print(
                    "\nPipeline stopped because a stage failed. "
                    "Inspect its output and run log."
                )
                break

    except KeyboardInterrupt:
        record["status"] = "interrupted"
        print("\nPipeline interrupted by user.")

    finally:
        if record["status"] == "running":
            all_succeeded = (
                len(record["stages"]) == len(STAGES)
                and all(
                    stage["status"] == "success"
                    for stage in record["stages"]
                )
            )

            record["status"] = (
                "success" if all_succeeded else "failed"
            )

        record["finished_at_utc"] = utc_now()
        record["duration_seconds"] = round(
            sum(
                stage.get("duration_seconds", 0)
                for stage in record["stages"]
            ),
            3,
        )

        successful_stages = {
            stage["name"]
            for stage in record["stages"]
            if stage.get("status") == "success"
        }
        record["metrics"] = collect_metrics(
            successful_stages=successful_stages
        )

        try:
            save_run_record(record)
        except OSError as error:
            print(f"Could not save lineage report: {error}")
            record["status"] = "failed"

    print("\n" + "=" * 75)
    print("RETAILFLOW 2.0 PIPELINE SUMMARY")
    print("=" * 75)
    print(f"Run ID       : {run_id}")
    print(f"Status       : {record['status']}")
    print(
        f"Stages passed: "
        f"{sum(s['status'] == 'success' for s in record['stages'])}"
        f"/{len(STAGES)}"
    )
    print(f"Duration     : {record['duration_seconds']} seconds")
    print(f"Latest report: {LATEST_RUN_FILE}")
    print(f"Run history  : {RUN_HISTORY_FILE}")
    print(f"Log file     : {log_file}")

    if record["status"] != "success":
        return False

    return True


if __name__ == "__main__":
    try:
        success = run_retailflow_v2()
    except Exception as error:
        print(f"\nORCHESTRATION ERROR: {error}")
        success = False

    if not success:
        sys.exit(1)
