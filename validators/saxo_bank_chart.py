from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass(frozen=True)
class SaxoBankChartValidationResult:
    row_count: int
    bad_column_count: int
    bad_timestamp_count: int
    bad_number_count: int
    duplicate_timestamp_count: int
    is_time_ordered: bool
    min_timestamp: datetime | None
    max_timestamp: datetime | None


def validate_saxo_bank_ohlcv(
    ohlcv_df: pd.DataFrame,
) -> SaxoBankChartValidationResult:
    expected_columns = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    row_count = len(ohlcv_df)

    missing_columns = [
        column
        for column in expected_columns
        if column not in ohlcv_df.columns
    ]

    bad_column_count = len(missing_columns)

    if ohlcv_df.empty or missing_columns:
        return SaxoBankChartValidationResult(
            row_count=row_count,
            bad_column_count=bad_column_count,
            bad_timestamp_count=row_count if "timestamp" in missing_columns else 0,
            bad_number_count=0,
            duplicate_timestamp_count=0,
            is_time_ordered=True,
            min_timestamp=None,
            max_timestamp=None,
        )

    df = ohlcv_df.copy()

    timestamps = pd.to_datetime(
        df["timestamp"],
        utc=True,
        errors="coerce",
    )

    bad_timestamp_count = int(timestamps.isna().sum())

    bad_number_count = 0

    for column in ["open", "high", "low", "close"]:
        values = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        bad_number_count += int(values.isna().sum())

    duplicate_timestamp_count = int(timestamps.duplicated().sum())

    valid_timestamps = timestamps.dropna()

    min_timestamp = (
        valid_timestamps.min().to_pydatetime()
        if not valid_timestamps.empty
        else None
    )

    max_timestamp = (
        valid_timestamps.max().to_pydatetime()
        if not valid_timestamps.empty
        else None
    )

    is_time_ordered = bool(timestamps.is_monotonic_increasing)

    return SaxoBankChartValidationResult(
        row_count=row_count,
        bad_column_count=bad_column_count,
        bad_timestamp_count=bad_timestamp_count,
        bad_number_count=bad_number_count,
        duplicate_timestamp_count=duplicate_timestamp_count,
        is_time_ordered=is_time_ordered,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )
