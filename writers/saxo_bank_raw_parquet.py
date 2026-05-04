from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_saxo_bank_raw_chart_to_parquet(
    *,
    chart_rows: list[dict],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(chart_rows)

    df.to_parquet(
        output_path,
        index=False,
        engine="pyarrow",
    )

    return output_path
