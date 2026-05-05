from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd


EXPECTED_INTERACTIVE_BROKERS_OHLCV_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


@dataclass(frozen=True)
class InteractiveBrokersBarsValidationResult:
    row_count: int
    bad_column_count: int
    bad_timestamp_count: int
    bad_number_count: int
    duplicate_timestamp_count: int
    is_time_ordered: bool
    min_timestamp: datetime | None
    max_timestamp: datetime | None


def validate_interactive_brokers_ohlcv(
    ohlcv_df: pd.DataFrame,
) -> InteractiveBrokersBarsValidationResult:
    missing_columns = [
        column
        for column in EXPECTED_INTERACTIVE_BROKERS_OHLCV_COLUMNS
        if column not in ohlcv_df.columns
    ]

    timestamp_series = pd.to_datetime(
        ohlcv_df["timestamp"],
        utc=True,
        errors="coerce",
    ) if "timestamp" in ohlcv_df.columns else pd.Series(dtype="datetime64[ns, UTC]")

    numeric_columns = [
        column
        for column in ["open", "high", "low", "close"]
        if column in ohlcv_df.columns
    ]

    numeric_df = ohlcv_df[numeric_columns].apply(
        pd.to_numeric,
        errors="coerce",
    ) if numeric_columns else pd.DataFrame()

    bad_number_count = int(numeric_df.isna().sum().sum()) if not numeric_df.empty else 0

    duplicate_timestamp_count = (
        int(timestamp_series.duplicated().sum())
        if not timestamp_series.empty
        else 0
    )

    valid_timestamps = timestamp_series.dropna()

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

    is_time_ordered = (
        bool(timestamp_series.is_monotonic_increasing)
        if not timestamp_series.empty
        else True
    )

    return InteractiveBrokersBarsValidationResult(
        row_count=len(ohlcv_df),
        bad_column_count=len(missing_columns),
        bad_timestamp_count=int(timestamp_series.isna().sum()),
        bad_number_count=bad_number_count,
        duplicate_timestamp_count=duplicate_timestamp_count,
        is_time_ordered=is_time_ordered,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )
