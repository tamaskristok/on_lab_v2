from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_INTERACTIVE_BROKERS_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def transform_interactive_brokers_bars_to_ohlcv(
    bars: list[dict],
) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

    df = pd.DataFrame(bars)

    missing_columns = [
        column
        for column in REQUIRED_INTERACTIVE_BROKERS_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing Interactive Brokers columns: {missing_columns}")

    ohlcv_df = df.rename(columns={"date": "timestamp"}).copy()

    ohlcv_df["timestamp"] = pd.to_datetime(
        ohlcv_df["timestamp"],
        utc=True,
        errors="coerce",
    )

    for column in ["open", "high", "low", "close", "volume"]:
        ohlcv_df[column] = pd.to_numeric(
            ohlcv_df[column],
            errors="coerce",
        )

    ohlcv_df["volume"] = ohlcv_df["volume"].replace(-1, np.nan)

    ohlcv_df = ohlcv_df[
        ["timestamp", "open", "high", "low", "close", "volume"]
    ].copy()

    ohlcv_df = ohlcv_df.dropna(
        subset=["timestamp", "open", "high", "low", "close"]
    )

    ohlcv_df = (
        ohlcv_df
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    return ohlcv_df
