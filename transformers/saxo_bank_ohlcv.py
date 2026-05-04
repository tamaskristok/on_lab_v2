from __future__ import annotations

import pandas as pd


SAXO_CHART_COLUMNS = [
    "Time",
    "OpenAsk",
    "OpenBid",
    "HighAsk",
    "HighBid",
    "LowAsk",
    "LowBid",
    "CloseAsk",
    "CloseBid",
]


def transform_saxo_bank_chart_to_ohlcv(
    chart_rows: list[dict],
) -> pd.DataFrame:
    if not chart_rows:
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

    df = pd.DataFrame(chart_rows)

    missing_columns = [
        column
        for column in SAXO_CHART_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing Saxo chart columns: {missing_columns}")

    ohlcv = pd.DataFrame()

    ohlcv["timestamp"] = pd.to_datetime(
        df["Time"],
        utc=True,
        errors="coerce",
    )

    for column in [
        "OpenAsk",
        "OpenBid",
        "HighAsk",
        "HighBid",
        "LowAsk",
        "LowBid",
        "CloseAsk",
        "CloseBid",
    ]:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    ohlcv["open"] = (df["OpenAsk"] + df["OpenBid"]) / 2
    ohlcv["high"] = (df["HighAsk"] + df["HighBid"]) / 2
    ohlcv["low"] = (df["LowAsk"] + df["LowBid"]) / 2
    ohlcv["close"] = (df["CloseAsk"] + df["CloseBid"]) / 2

    ohlcv["volume"] = float("nan")

    ohlcv = ohlcv.dropna(
        subset=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
        ]
    )

    ohlcv = (
        ohlcv
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    return ohlcv
