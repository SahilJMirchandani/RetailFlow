"""Self-contained unit tests for RetailFlow 2.0 orchestration."""

import importlib
import json
import sys
from pathlib import Path

orchestrator = importlib.import_module("pipeline.run_retailflow_v2")


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_collect_metrics_excludes_reports_from_unsuccessful_stages(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(orchestrator, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(orchestrator, "OUTPUT_DIR", tmp_path / "data" / "output")

    write_json(
        tmp_path / "data/output/product_catalog_quality.json",
        {
            "records_received": 194,
            "distinct_categories": 24,
            "passed": True,
        },
    )
    write_json(
        tmp_path / "data/output/olist_gold_quality.json",
        {"passed": True, "row_counts": {"fact_orders": 99_441}},
    )

    metrics = orchestrator.collect_metrics(
        successful_stages={"Product Catalogue API Ingestion"}
    )

    assert metrics["product_catalog"]["records"] == 194
    assert "olist_gold" not in metrics


def test_collect_metrics_returns_gold_report_after_success(monkeypatch, tmp_path):
    monkeypatch.setattr(orchestrator, "PROJECT_ROOT", tmp_path)
    write_json(
        tmp_path / "data/output/olist_gold_quality.json",
        {
            "passed": True,
            "row_counts": {"fact_orders": 99_441},
            "checks": {"all_fact_foreign_keys_resolve": True},
            "reconciliation": {"source_item_revenue": 13_591_643.70},
        },
    )

    metrics = orchestrator.collect_metrics(
        successful_stages={"Olist Gold Warehouse"}
    )

    assert metrics["olist_gold"]["quality_passed"] is True
    assert metrics["olist_gold"]["row_counts"]["fact_orders"] == 99_441


def test_execute_stage_records_success_and_captures_log(
    monkeypatch, tmp_path, capsys
):
    script = tmp_path / "scripts" / "ok.py"
    script.parent.mkdir(parents=True)
    script.write_text('print("stage completed")\n', encoding="utf-8")
    monkeypatch.setattr(orchestrator, "PROJECT_ROOT", tmp_path)
    log_file = tmp_path / "run.log"

    result = orchestrator.execute_stage(
        {"name": "Test Stage", "script": "scripts/ok.py",
         "inputs": [], "outputs": []},
        log_file,
    )

    assert result["status"] == "success"
    assert result["exit_code"] == 0
    assert "stage completed" in log_file.read_text(encoding="utf-8")
    assert "stage completed" in capsys.readouterr().out


def test_execute_stage_records_nonzero_exit_code(monkeypatch, tmp_path):
    script = tmp_path / "scripts" / "fail.py"
    script.parent.mkdir(parents=True)
    script.write_text("raise SystemExit(7)\n", encoding="utf-8")
    monkeypatch.setattr(orchestrator, "PROJECT_ROOT", tmp_path)

    result = orchestrator.execute_stage(
        {"name": "Failing Stage", "script": "scripts/fail.py",
         "inputs": [], "outputs": []},
        tmp_path / "run.log",
    )

    assert result["status"] == "failed"
    assert result["exit_code"] == 7


def test_save_run_record_updates_latest_and_appends_history(
    monkeypatch, tmp_path
):
    lineage_dir = tmp_path / "data" / "output" / "lineage"
    monkeypatch.setattr(orchestrator, "LINEAGE_DIR", lineage_dir)
    monkeypatch.setattr(
        orchestrator, "LATEST_RUN_FILE", lineage_dir / "pipeline_run_latest.json"
    )
    monkeypatch.setattr(
        orchestrator, "RUN_HISTORY_FILE", lineage_dir / "pipeline_runs.jsonl"
    )

    record = {"run_id": "unit-test-run", "status": "success"}
    orchestrator.save_run_record(record)

    assert json.loads(
        (lineage_dir / "pipeline_run_latest.json").read_text(encoding="utf-8")
    ) == record
    lines = (lineage_dir / "pipeline_runs.jsonl").read_text(
        encoding="utf-8"
    ).splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == record

    orchestrator.save_run_record({"run_id": "second-run"})
    lines = (lineage_dir / "pipeline_runs.jsonl").read_text(
        encoding="utf-8"
    ).splitlines()
    assert len(lines) == 2
