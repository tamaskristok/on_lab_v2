from __future__ import annotations

import lzma
import struct
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from providers.dukascopy import DukascopyDownloadResult


def decode_dukascopy_bi5(
    bi5_path: Path,
    hour_start: datetime,
    price_scale: int,
) -> pd.DataFrame:
    compressed = bi5_path.read_bytes()
    decompressed = lzma.decompress(compressed)

    record_size = 20

    if len(decompressed) % record_size != 0:
        raise ValueError("Invalid Dukascopy BI5 file size.")

    rows = []

    for offset in range(0, len(decompressed), record_size):
        time_delta_ms, ask, bid, ask_volume, bid_volume = struct.unpack(
            ">iii ff",
            decompressed[offset : offset + record_size],
        )

        rows.append(
            {
                "timestamp": hour_start + timedelta(milliseconds=time_delta_ms),
                "bid": bid / price_scale,
                "ask": ask / price_scale,
                "bid_volume": bid_volume,
                "ask_volume": ask_volume,
            }
        )

    return pd.DataFrame(rows)


def decode_dukascopy_downloads(
    downloads: list[DukascopyDownloadResult],
    price_scale: int,
) -> pd.DataFrame:
    tick_frames = []

    for download in downloads:
        if download.is_empty:
            continue

        tick_df = decode_dukascopy_bi5(
            bi5_path=download.file,
            hour_start=download.hour_start,
            price_scale=price_scale,
        )

        if not tick_df.empty:
            tick_frames.append(tick_df)

    if not tick_frames:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "bid",
                "ask",
                "bid_volume",
                "ask_volume",
            ]
        )

    return (
        pd.concat(tick_frames, ignore_index=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
