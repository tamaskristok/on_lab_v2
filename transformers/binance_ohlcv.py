from __future__ import annotations

from pathlib import Path

import pandas as pd


BINANCE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def transform_binance_csv_to_ohlcv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        csv_path,
        header=None,
        names=BINANCE_COLUMNS,
    )

    ohlcv = pd.DataFrame()

    ohlcv["timestamp"] = pd.to_datetime(
        df["open_time"],
        unit="ms",
        utc=True,
        errors="coerce",
    )

    ohlcv["open"] = pd.to_numeric(df["open"], errors="coerce")
    ohlcv["high"] = pd.to_numeric(df["high"], errors="coerce")
    ohlcv["low"] = pd.to_numeric(df["low"], errors="coerce")
    ohlcv["close"] = pd.to_numeric(df["close"], errors="coerce")
    ohlcv["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    ohlcv = ohlcv.dropna(
        subset=["timestamp", "open", "high", "low", "close", "volume"]
    )

    ohlcv = ohlcv.sort_values("timestamp").reset_index(drop=True)

    return ohlcv
