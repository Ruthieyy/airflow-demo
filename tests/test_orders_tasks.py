from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from orders_tasks import DataQualityError, etl, ingest, quality, validate_orders

ROOT = Path(__file__).resolve().parents[1]


def _use_raw_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: Path) -> None:
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    shutil.copy(source, raw_dir / "orders_sample.csv")
    monkeypatch.setenv("DATA_PIPELINE_HOME", str(tmp_path))


def test_sample_csv_runs_ingest_quality_and_etl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _use_raw_file(tmp_path, monkeypatch, ROOT / "data" / "raw" / "orders_sample.csv")

    staging = ingest()
    report = quality()
    parquet_path = etl()

    assert Path(staging).is_file()
    assert report["passed"] is True
    assert report["row_count"] == 5

    written_report = json.loads((tmp_path / "data" / "staging" / "quality_report.json").read_text())
    assert written_report["passed"] is True

    curated = pd.read_parquet(parquet_path)
    csv_frame = pd.read_csv(tmp_path / "data" / "curated" / "orders.csv", dtype="string")
    assert list(curated["order_id"]) == ["1001", "1002", "1003", "1004", "1005"]
    assert curated["amount"].tolist() == pytest.approx([29.99, 15.00, 0.00, 120.50, 8.25])
    assert curated["order_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
    ]
    assert csv_frame["status"].tolist() == ["shipped", "pending", "cancelled", "returned", "shipped"]


def test_quality_fails_on_bad_sample(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _use_raw_file(tmp_path, monkeypatch, ROOT / "data" / "raw" / "orders_bad_sample.csv")

    ingest()
    with pytest.raises(DataQualityError) as raised:
        quality()

    message = str(raised.value)
    assert "duplicate order_id" in message
    assert "blank customer_id" in message
    assert "amount" in message
    assert "order_date" in message
    assert "status" in message
    assert "blank order_id" in message

    report = json.loads((tmp_path / "data" / "staging" / "quality_report.json").read_text())
    assert report["passed"] is False
    assert not (tmp_path / "data" / "curated" / "orders.parquet").exists()


def test_etl_refuses_bad_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _use_raw_file(tmp_path, monkeypatch, ROOT / "data" / "raw" / "orders_bad_sample.csv")
    ingest()

    with pytest.raises(DataQualityError):
        etl()

    assert not (tmp_path / "data" / "curated" / "orders.parquet").exists()


def test_validate_orders_reports_missing_column() -> None:
    frame = pd.DataFrame({"order_id": ["1"]})
    errors = validate_orders(frame)
    assert errors == [
        "missing required columns: customer_id, amount, order_date, status"
    ]


def test_ingest_fails_when_raw_file_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_PIPELINE_HOME", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        ingest()
