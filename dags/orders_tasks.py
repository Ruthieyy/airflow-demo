"""Local orders tasks: stage a sample CSV, check rows, write curated files.

Paths default to the repository ``data/`` directory. Set ``DATA_PIPELINE_HOME``
to point the same layout at another root.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ("order_id", "customer_id", "amount", "order_date", "status")
ALLOWED_STATUS = frozenset({"shipped", "pending", "cancelled", "returned"})


class DataQualityError(ValueError):
    """A dataset failed a check. The calling task should fail."""


def repo_root() -> Path:
    override = os.environ.get("DATA_PIPELINE_HOME")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1]


def pipeline_paths(root: Path | None = None) -> dict[str, Path]:
    root = repo_root() if root is None else root
    return {
        "raw": root / "data" / "raw" / "orders_sample.csv",
        "staging": root / "data" / "staging" / "orders.csv",
        "report": root / "data" / "staging" / "quality_report.json",
        "curated_csv": root / "data" / "curated" / "orders.csv",
        "curated_parquet": root / "data" / "curated" / "orders.parquet",
    }


def _stripped(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def validate_orders(df: pd.DataFrame) -> list[str]:
    """Return human-readable failures. An empty list means the frame is acceptable."""
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        return [f"missing required columns: {', '.join(missing)}"]
    if df.empty:
        return ["dataset has no rows"]

    order_id = _stripped(df["order_id"])
    customer_id = _stripped(df["customer_id"])
    amount_text = _stripped(df["amount"])
    order_date = _stripped(df["order_date"])
    status = _stripped(df["status"])

    errors: list[str] = []

    blank_order = order_id.isna() | (order_id == "")
    if blank_order.any():
        errors.append(f"blank order_id on {int(blank_order.sum())} row(s)")

    duplicate_mask = order_id.duplicated(keep=False) & ~blank_order
    if duplicate_mask.any():
        duplicate_ids = sorted(set(order_id[duplicate_mask].astype(str)))
        errors.append(f"duplicate order_id values: {', '.join(duplicate_ids)}")

    blank_customer = customer_id.isna() | (customer_id == "")
    if blank_customer.any():
        errors.append(f"blank customer_id on {int(blank_customer.sum())} row(s)")

    amounts = pd.to_numeric(amount_text, errors="coerce")
    bad_amount = amounts.isna() | (amounts < 0)
    if bad_amount.any():
        errors.append(
            f"amount is missing, non-numeric, or negative on {int(bad_amount.sum())} row(s)"
        )

    parsed_dates = pd.to_datetime(order_date, format="%Y-%m-%d", errors="coerce")
    bad_dates = parsed_dates.isna()
    if bad_dates.any():
        errors.append(f"order_date is not YYYY-MM-DD on {int(bad_dates.sum())} row(s)")

    bad_status = status.isna() | ~status.str.lower().isin(ALLOWED_STATUS)
    if bad_status.any():
        allowed = ", ".join(sorted(ALLOWED_STATUS))
        errors.append(f"status not in {{{allowed}}} on {int(bad_status.sum())} row(s)")

    return errors


def _read_raw(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Raw orders CSV not found: {path}")
    return pd.read_csv(path, dtype="string")


def ingest() -> str:
    """Copy the sample CSV into staging. Fails when the file is missing or empty."""
    paths = pipeline_paths()
    frame = _read_raw(paths["raw"])
    if frame.empty:
        raise ValueError(f"Raw orders CSV has no rows: {paths['raw']}")
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(
            f"Raw orders CSV is missing columns: {', '.join(missing)}"
        )
    paths["staging"].parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(paths["staging"], index=False)
    logger.info("Ingested %s rows from %s to %s", len(frame), paths["raw"], paths["staging"])
    return str(paths["staging"])


def quality() -> dict[str, object]:
    """Validate staged orders. Raise DataQualityError when any check fails."""
    paths = pipeline_paths()
    if not paths["staging"].is_file():
        raise FileNotFoundError(
            f"Staging file not found: {paths['staging']}. Run ingest first."
        )
    frame = pd.read_csv(paths["staging"], dtype="string")
    errors = validate_orders(frame)
    report: dict[str, object] = {
        "row_count": int(len(frame)),
        "passed": not errors,
        "errors": errors,
    }
    paths["report"].write_text(json.dumps(report, indent=2) + "\n")
    if errors:
        raise DataQualityError("data quality failed: " + "; ".join(errors))
    logger.info("Quality check passed for %s rows", len(frame))
    return report


def _curate(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": _stripped(frame["order_id"]),
            "customer_id": _stripped(frame["customer_id"]),
            "amount": pd.to_numeric(_stripped(frame["amount"])),
            "order_date": pd.to_datetime(_stripped(frame["order_date"]), format="%Y-%m-%d"),
            "status": _stripped(frame["status"]).str.lower(),
        }
    )


def etl() -> str:
    """Write curated CSV and Parquet. Refuses to write when validation fails."""
    paths = pipeline_paths()
    if not paths["staging"].is_file():
        raise FileNotFoundError(
            f"Staging file not found: {paths['staging']}. Run ingest and quality first."
        )
    frame = pd.read_csv(paths["staging"], dtype="string")
    errors = validate_orders(frame)
    if errors:
        raise DataQualityError(
            "refusing to write curated output; data quality failed: " + "; ".join(errors)
        )
    curated = _curate(frame)
    paths["curated_csv"].parent.mkdir(parents=True, exist_ok=True)
    csv_frame = curated.copy()
    csv_frame["order_date"] = csv_frame["order_date"].dt.strftime("%Y-%m-%d")
    csv_frame.to_csv(paths["curated_csv"], index=False)
    curated.to_parquet(paths["curated_parquet"], index=False, engine="pyarrow")
    logger.info(
        "Wrote %s curated rows to %s and %s",
        len(curated),
        paths["curated_csv"],
        paths["curated_parquet"],
    )
    return str(paths["curated_parquet"])
