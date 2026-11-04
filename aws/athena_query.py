"""Run safe, read-only exploratory queries against RetailFlow's Glue tables."""
from __future__ import annotations

import argparse
import re
import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws.config import require_aws_settings

DEFAULT_SQL = (
    "SELECT order_date, product_revenue, order_count "
    "FROM gold_daily_sales ORDER BY order_date DESC LIMIT 10"
)


def validate_read_only_sql(sql: str) -> str:
    query = sql.strip()
    if query.endswith(";"):
        query = query[:-1].rstrip()
    if not query or ";" in query or re.search(r"--|/\*|\*/", query):
        raise ValueError("Provide one read-only SQL statement without comments or extra statements.")
    if not re.match(r"^(SELECT|SHOW|DESCRIBE|EXPLAIN)\b", query, re.IGNORECASE):
        raise ValueError("Only SELECT, SHOW, DESCRIBE and EXPLAIN statements are allowed.")
    if query.upper().startswith("SELECT ") and not re.search(r"\bLIMIT\s+\d+\b", query, re.IGNORECASE):
        raise ValueError("Add LIMIT <n> to SELECT queries to keep exploratory result sets bounded.")
    return query


def execute_query(sql: str, timeout_seconds: int = 120) -> dict:
    settings = require_aws_settings()
    query = validate_read_only_sql(sql)
    profile = __import__("os").getenv("AWS_PROFILE")
    session_args = {"region_name": settings["region"]}
    if profile:
        session_args["profile_name"] = profile
    athena = boto3.Session(**session_args).client("athena")

    started = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": settings["database"]},
        WorkGroup=settings["workgroup"],
        ResultConfiguration={"OutputLocation": settings["athena_output"]},
    )
    query_id = started["QueryExecutionId"]
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        state = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(1)
    else:
        athena.stop_query_execution(QueryExecutionId=query_id)
        raise TimeoutError(f"Athena query timed out and was stopped: {query_id}")

    execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    status = execution["Status"]
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(
            f"Athena query {status['State']}: {status.get('StateChangeReason', 'No reason supplied')}"
        )

    paginator = athena.get_paginator("get_query_results")
    rows = []
    column_names = []
    for page in paginator.paginate(QueryExecutionId=query_id):
        page_rows = page.get("ResultSet", {}).get("Rows", [])
        if not column_names and page_rows:
            column_names = [cell.get("VarCharValue", "") for cell in page_rows[0]["Data"]]
            page_rows = page_rows[1:]
        for row in page_rows:
            rows.append([cell.get("VarCharValue", "") for cell in row.get("Data", [])])

    statistics = execution.get("Statistics", {})
    return {
        "query_id": query_id,
        "state": status["State"],
        "sql": query,
        "columns": column_names,
        "rows": rows,
        "data_scanned_bytes": statistics.get("DataScannedInBytes", 0),
        "execution_time_ms": statistics.get("EngineExecutionTimeInMillis", 0),
        "output_location": execution.get("ResultConfiguration", {}).get("OutputLocation"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql", default=DEFAULT_SQL, help="Single read-only SQL query.")
    parser.add_argument("--timeout", type=int, default=120, help="Maximum wait time in seconds.")
    args = parser.parse_args()
    try:
        result = execute_query(args.sql, timeout_seconds=args.timeout)
    except (BotoCoreError, ClientError, OSError, RuntimeError, TimeoutError, ValueError) as error:
        print(f"ATHENA QUERY FAILED: {error}", file=sys.stderr)
        return 1

    print(f"Query ID: {result['query_id']}")
    print(f"State: {result['state']}")
    print(f"Data scanned: {result['data_scanned_bytes']:,} bytes")
    print(f"Execution: {result['execution_time_ms']:,} ms")
    print(f"Output: {result['output_location']}")
    if result["columns"]:
        print(" | ".join(result["columns"]))
        print("-" * 80)
        for row in result["rows"]:
            print(" | ".join(row))
    else:
        print("Query completed with no result rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
