from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_interactive_brokers_raw_bars_to_parquet(
    *,
    bars: list[dict],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(bars).to_parquet(
        output_path,
        index=False,
        engine="pyarrow",
    )

    return output_path
