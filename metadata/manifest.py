from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def build_manifest(
    *,
    provider: str,
    symbol: str,
    interval: str,
    year: int,
    month: int,
    source_url: str,
    raw_file_path: Path,
    parquet_file_path: Path,
    validation_result,
    ohlcv_df: pd.DataFrame,
    ingestion_errors: list[dict] | None = None,
) -> dict:
    if ingestion_errors is None:
        ingestion_errors = []

    normalized_ingestion_errors = []

    for error in ingestion_errors:
        normalized_error = {}

        for key, value in error.items():
            if hasattr(value, "isoformat"):
                normalized_error[key] = value.isoformat()
            else:
                normalized_error[key] = value

        normalized_ingestion_errors.append(normalized_error)

    if "timestamp" not in ohlcv_df.columns:
        min_data_timestamp = None
        max_data_timestamp = None
    else:
        data_timestamps = pd.to_datetime(
            ohlcv_df["timestamp"],
            utc=True,
            errors="coerce",
        ).dropna()

        if data_timestamps.empty:
            min_data_timestamp = None
            max_data_timestamp = None
        else:
            min_data_timestamp = data_timestamps.min().isoformat()
            max_data_timestamp = data_timestamps.max().isoformat()

    return {
        "metadata": {
            "provider": provider,
            "symbol": symbol,
            "interval": interval,
            "year": year,
            "month": month,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "source": {
            "url": source_url,
            "raw_file": str(raw_file_path),
        },
        "output": {
            "parquet_file": str(parquet_file_path),
        },
        "validation": {
            "row_count": validation_result.row_count,
            "bad_column_count": validation_result.bad_column_count,
            "bad_timestamp_count": validation_result.bad_timestamp_count,
            "bad_number_count": validation_result.bad_number_count,
            "duplicate_timestamp_count": validation_result.duplicate_timestamp_count,
            "is_time_ordered": validation_result.is_time_ordered,
            "min_timestamp": (
                validation_result.min_timestamp.isoformat()
                if validation_result.min_timestamp is not None
                else None
            ),
            "max_timestamp": (
                validation_result.max_timestamp.isoformat()
                if validation_result.max_timestamp is not None
                else None
            ),
        },
        "ingestion": {
            "error_count": len(normalized_ingestion_errors),
            "errors": normalized_ingestion_errors,
        },
        "data": {
            "row_count": len(ohlcv_df),
            "min_timestamp": min_data_timestamp,
            "max_timestamp": max_data_timestamp,
            "columns": list(ohlcv_df.columns),
        },
    }


def write_manifest(
    manifest: dict,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            manifest,
            file,
            indent=4,
            ensure_ascii=False,
        )

    return output_path
