"""Unit tests for portable Online Retail dataset ingestion."""

import importlib
from pathlib import Path

import pytest

ingest_module = importlib.import_module("ingestion.ingest_data")


def test_resolve_source_prefers_canonical_raw_file(monkeypatch, tmp_path):
    raw_file = tmp_path / "data" / "raw" / "Online Retail.xlsx"
    raw_file.parent.mkdir(parents=True)
    raw_file.write_bytes(b"workbook")
    monkeypatch.delenv(ingest_module.SOURCE_ENV_VAR, raising=False)
    monkeypatch.setattr(ingest_module, "RAW_FILE", raw_file)
    monkeypatch.setattr(
        ingest_module, "LEGACY_SOURCE_FILE", tmp_path / "missing-legacy.xlsx"
    )

    assert ingest_module.resolve_source_file() == raw_file.resolve()


def test_invalid_configured_source_does_not_silently_fallback(monkeypatch, tmp_path):
    raw_file = tmp_path / "Online Retail.xlsx"
    raw_file.write_bytes(b"existing workbook")
    monkeypatch.setattr(ingest_module, "RAW_FILE", raw_file)
    monkeypatch.setattr(
        ingest_module, "LEGACY_SOURCE_FILE", raw_file
    )
    monkeypatch.setenv(
        ingest_module.SOURCE_ENV_VAR, str(tmp_path / "does-not-exist.xlsx")
    )

    assert ingest_module.resolve_source_file() is None


def test_ingest_copies_configured_source(monkeypatch, tmp_path):
    source = tmp_path / "source.xlsx"
    source.write_bytes(b"test workbook bytes")
    raw_dir = tmp_path / "project" / "data" / "raw"
    raw_file = raw_dir / "Online Retail.xlsx"
    monkeypatch.setenv(ingest_module.SOURCE_ENV_VAR, str(source))
    monkeypatch.setattr(ingest_module, "RAW_DIR", raw_dir)
    monkeypatch.setattr(ingest_module, "RAW_FILE", raw_file)

    assert ingest_module.ingest_data()
    assert raw_file.read_bytes() == source.read_bytes()


def test_ingest_succeeds_when_workbook_is_already_in_raw(monkeypatch, tmp_path):
    raw_file = tmp_path / "data" / "raw" / "Online Retail.xlsx"
    raw_file.parent.mkdir(parents=True)
    raw_file.write_bytes(b"already in place")
    monkeypatch.delenv(ingest_module.SOURCE_ENV_VAR, raising=False)
    monkeypatch.setattr(ingest_module, "RAW_DIR", raw_file.parent)
    monkeypatch.setattr(ingest_module, "RAW_FILE", raw_file)
    monkeypatch.setattr(
        ingest_module, "LEGACY_SOURCE_FILE", tmp_path / "missing-legacy.xlsx"
    )

    assert ingest_module.ingest_data()
    assert raw_file.read_bytes() == b"already in place"
