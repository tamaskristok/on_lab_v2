from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_ohlcv_to_parquet(
    df: pd.DataFrame,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        index=False,
        engine="pyarrow",
    )

    return output_path
