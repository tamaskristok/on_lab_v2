from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _timestamp_to_iso_or_none(
    value,
) -> str | None:
    if value is None:
        return None

    if pd.isna(value):
        return None

    return pd.to_datetime(
        value,
        utc=True,
    ).isoformat()


def build_silver_generated_ohlcv_manifest(
    *,
    asset: str,
    interval: str,
    year: int,
    month: int,
    generation_method_id: int,
    generation_method: str,
    parquet_file_path: Path,
    silver_ohlcv_df: pd.DataFrame,
) -> dict:
    row_count = len(silver_ohlcv_df)

    generated_rows = int(
        silver_ohlcv_df["is_generated"].sum()
    )

    missing_rows = int(
        (silver_ohlcv_df["consensus_quality"] == "missing").sum()
    )

    single_source_rows = int(
        (silver_ohlcv_df["consensus_quality"] == "single_source").sum()
    )

    multi_source_rows = int(
        (silver_ohlcv_df["consensus_quality"] == "multi_source").sum()
    )

    min_timestamp = None
    max_timestamp = None

    if row_count > 0:
        min_timestamp = _timestamp_to_iso_or_none(
            silver_ohlcv_df["timestamp"].min()
        )
        max_timestamp = _timestamp_to_iso_or_none(
            silver_ohlcv_df["timestamp"].max()
        )

    return {
        "metadata": {
            "layer": "silver",
            "dataset": "generated_ohlcv",
            "asset": asset,
            "interval": interval,
            "year": year,
            "month": month,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "generation": {
            "method_id": generation_method_id,
            "method": generation_method,
        },
        "output": {
            "parquet_file": str(parquet_file_path),
        },
        "quality": {
            "row_count": row_count,
            "generated_rows": generated_rows,
            "missing_rows": missing_rows,
            "single_source_rows": single_source_rows,
            "multi_source_rows": multi_source_rows,
            "min_timestamp": min_timestamp,
            "max_timestamp": max_timestamp,
        },
        "columns": list(silver_ohlcv_df.columns),
    }


def write_silver_manifest(
    *,
    manifest: dict,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_text(
        json.dumps(
            manifest,
            indent=4,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return output_path
