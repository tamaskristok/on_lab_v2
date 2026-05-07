from __future__ import annotations

import pandas as pd


def _extract_broker_names(
    generated_df: pd.DataFrame,
) -> list[str]:
    broker_names = []

    for column in generated_df.columns:
        if not column.endswith("_close"):
            continue

        if column.startswith("generated_"):
            continue

        if column in {
            "close_min",
            "close_max",
        }:
            continue

        broker_names.append(
            column.removesuffix("_close")
        )

    return sorted(broker_names)


def build_broker_ranking(
    generated_df: pd.DataFrame,
) -> pd.DataFrame:
    records = []

    broker_names = _extract_broker_names(
        generated_df,
    )

    for asset, asset_df in generated_df.groupby("asset"):
        asset_row_count = len(asset_df)

        for broker in broker_names:
            close_column = f"{broker}_close"

            if close_column not in asset_df.columns:
                continue

            broker_present_mask = asset_df[close_column].notna()
            broker_rows = int(broker_present_mask.sum())

            if broker_rows == 0:
                continue

            generated_close = asset_df["generated_close"]
            broker_close = asset_df[close_column]

            abs_diff = (
                broker_close - generated_close
            ).abs()

            abs_diff_pct = (
                abs_diff / generated_close * 100
            )

            records.append(
                {
                    "asset": asset,
                    "broker": broker,
                    "calendar_rows": asset_row_count,
                    "broker_rows": broker_rows,
                    "missing_rows": asset_row_count - broker_rows,
                    "coverage_ratio": broker_rows / asset_row_count,
                    "mean_abs_diff": abs_diff.mean(),
                    "median_abs_diff": abs_diff.median(),
                    "max_abs_diff": abs_diff.max(),
                    "mean_abs_diff_pct": abs_diff_pct.mean(),
                    "median_abs_diff_pct": abs_diff_pct.median(),
                    "max_abs_diff_pct": abs_diff_pct.max(),
                }
            )

    if not records:
        return pd.DataFrame(
            columns=[
                "asset",
                "broker",
                "calendar_rows",
                "broker_rows",
                "missing_rows",
                "coverage_ratio",
                "mean_abs_diff",
                "median_abs_diff",
                "max_abs_diff",
                "mean_abs_diff_pct",
                "median_abs_diff_pct",
                "max_abs_diff_pct",
            ]
        )

    ranking_df = pd.DataFrame(records)

    return (
        ranking_df
        .sort_values(
            [
                "asset",
                "coverage_ratio",
                "mean_abs_diff_pct",
            ],
            ascending=[
                True,
                False,
                True,
            ],
        )
        .reset_index(drop=True)
    )
