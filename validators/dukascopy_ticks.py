from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass(frozen=True)
class DukascopyTickValidationResult:
    row_count: int
    bad_column_count: int
    bad_timestamp_count: int
    bad_number_count: int
    duplicate_timestamp_count: int
    is_time_ordered: bool
    min_timestamp: datetime | None
    max_timestamp: datetime | None


def validate_dukascopy_ticks(
    ticks_df: pd.DataFrame,
) -> DukascopyTickValidationResult:
    expected_columns = [
        "timestamp",
        "bid",
        "ask",
        "bid_volume",
        "ask_volume",
    ]

    row_count = len(ticks_df)

    missing_columns = [
        column
        for column in expected_columns
        if column not in ticks_df.columns
    ]

    bad_column_count = len(missing_columns)

    if ticks_df.empty or missing_columns:
        return DukascopyTickValidationResult(
            row_count=row_count,
            bad_column_count=bad_column_count,
            bad_timestamp_count=row_count if "timestamp" in missing_columns else 0,
            bad_number_count=0,
            duplicate_timestamp_count=0,
            is_time_ordered=True,
            min_timestamp=None,
            max_timestamp=None,
        )

    df = ticks_df.copy()

    timestamp = pd.to_datetime(
        df["timestamp"],
        utc=True,
        errors="coerce",
    )

    bad_timestamp_count = int(timestamp.isna().sum())

    number_columns = [
        "bid",
        "ask",
        "bid_volume",
        "ask_volume",
    ]

    bad_number_count = 0

    for column in number_columns:
        values = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        bad_number_count += int(values.isna().sum())

    duplicate_timestamp_count = int(timestamp.duplicated().sum())

    valid_timestamp = timestamp.dropna()

    min_timestamp = (
        valid_timestamp.min().to_pydatetime()
        if not valid_timestamp.empty
        else None
    )

    max_timestamp = (
        valid_timestamp.max().to_pydatetime()
        if not valid_timestamp.empty
        else None
    )

    is_time_ordered = bool(timestamp.is_monotonic_increasing)

    return DukascopyTickValidationResult(
        row_count=row_count,
        bad_column_count=bad_column_count,
        bad_timestamp_count=bad_timestamp_count,
        bad_number_count=bad_number_count,
        duplicate_timestamp_count=duplicate_timestamp_count,
        is_time_ordered=is_time_ordered,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )
