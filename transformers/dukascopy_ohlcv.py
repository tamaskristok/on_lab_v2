from __future__ import annotations

import pandas as pd


def _normalize_resample_interval(interval: str) -> str:
    if interval.endswith("m") and interval[:-1].isdigit():
        return f"{interval[:-1]}min"

    return interval


def transform_dukascopy_ticks_to_ohlcv(
    ticks_df: pd.DataFrame,
    interval: str = "1min",
) -> pd.DataFrame:
    required_columns = [
        "timestamp",
        "bid",
        "ask",
        "bid_volume",
        "ask_volume",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in ticks_df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if ticks_df.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )

    df = ticks_df.copy()

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        utc=True,
        errors="coerce",
    )

    for column in ["bid", "ask", "bid_volume", "ask_volume"]:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    df = df.dropna(
        subset=[
            "timestamp",
            "bid",
            "ask",
            "bid_volume",
            "ask_volume",
        ]
    )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )

    df["mid"] = (df["bid"] + df["ask"]) / 2
    df["volume"] = df["bid_volume"] + df["ask_volume"]

    resample_interval = _normalize_resample_interval(interval)

    ohlcv = (
        df.set_index("timestamp")
        .resample(resample_interval)
        .agg(
            open=("mid", "first"),
            high=("mid", "max"),
            low=("mid", "min"),
            close=("mid", "last"),
            volume=("volume", "sum"),
        )
        .dropna()
        .reset_index()
    )

    return ohlcv
