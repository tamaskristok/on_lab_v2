from __future__ import annotations

from pathlib import Path

import pandas as pd


SILVER_OHLCV_COLUMNS = [
    "timestamp",
    "asset",
    "year",
    "month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "is_generated",
    "consensus_quality",
    "generation_method_id",
    "generation_method",
    "broker_count",
    "close_diff",
    "close_diff_pct",
    "candle_quality",
    "return_pct",
    "abs_return_pct",
    "is_outlier",
    "quality_status",
]


DEFAULT_GOOD_CLOSE_DIFF_PCT_THRESHOLD = 0.05
DEFAULT_WARNING_CLOSE_DIFF_PCT_THRESHOLD = 0.25
DEFAULT_OUTLIER_ABS_RETURN_PCT_THRESHOLD = 1.0


def load_silver_quality_thresholds(
    *,
    config_dir: Path = Path("config"),
) -> pd.DataFrame:
    threshold_path = config_dir / "silver_quality_thresholds.csv"

    if not threshold_path.exists():
        return pd.DataFrame(
            [
                {
                    "asset": "DEFAULT",
                    "good_close_diff_pct_threshold": (
                        DEFAULT_GOOD_CLOSE_DIFF_PCT_THRESHOLD
                    ),
                    "warning_close_diff_pct_threshold": (
                        DEFAULT_WARNING_CLOSE_DIFF_PCT_THRESHOLD
                    ),
                    "outlier_abs_return_pct_threshold": (
                        DEFAULT_OUTLIER_ABS_RETURN_PCT_THRESHOLD
                    ),
                }
            ]
        )

    thresholds_df = pd.read_csv(
        threshold_path,
    )

    thresholds_df["asset"] = thresholds_df["asset"].str.upper()

    return thresholds_df


def _get_default_thresholds(
    *,
    thresholds_df: pd.DataFrame,
) -> tuple[float, float, float]:
    default_rows = thresholds_df[
        thresholds_df["asset"] == "DEFAULT"
    ]

    if default_rows.empty:
        return (
            DEFAULT_GOOD_CLOSE_DIFF_PCT_THRESHOLD,
            DEFAULT_WARNING_CLOSE_DIFF_PCT_THRESHOLD,
            DEFAULT_OUTLIER_ABS_RETURN_PCT_THRESHOLD,
        )

    default_row = default_rows.iloc[0]

    return (
        float(default_row["good_close_diff_pct_threshold"]),
        float(default_row["warning_close_diff_pct_threshold"]),
        float(default_row["outlier_abs_return_pct_threshold"]),
    )


def _attach_quality_thresholds(
    *,
    silver_ohlcv_df: pd.DataFrame,
    thresholds_df: pd.DataFrame,
) -> pd.DataFrame:
    quality_df = silver_ohlcv_df.copy()

    good_default, warning_default, outlier_default = _get_default_thresholds(
        thresholds_df=thresholds_df,
    )

    asset_thresholds_df = thresholds_df[
        thresholds_df["asset"] != "DEFAULT"
    ].copy()

    quality_df = quality_df.merge(
        asset_thresholds_df,
        on="asset",
        how="left",
    )

    quality_df["good_close_diff_pct_threshold"] = quality_df[
        "good_close_diff_pct_threshold"
    ].fillna(good_default)

    quality_df["warning_close_diff_pct_threshold"] = quality_df[
        "warning_close_diff_pct_threshold"
    ].fillna(warning_default)

    quality_df["outlier_abs_return_pct_threshold"] = quality_df[
        "outlier_abs_return_pct_threshold"
    ].fillna(outlier_default)

    return quality_df


def add_candle_quality_flag(
    silver_ohlcv_df: pd.DataFrame,
    *,
    thresholds_df: pd.DataFrame | None = None,
    config_dir: Path = Path("config"),
) -> pd.DataFrame:
    if thresholds_df is None:
        thresholds_df = load_silver_quality_thresholds(
            config_dir=config_dir,
        )

    quality_df = _attach_quality_thresholds(
        silver_ohlcv_df=silver_ohlcv_df,
        thresholds_df=thresholds_df,
    )

    quality_df["candle_quality"] = "unknown"

    quality_df.loc[
        quality_df["consensus_quality"] == "missing",
        "candle_quality",
    ] = "missing"

    quality_df.loc[
        quality_df["consensus_quality"] == "single_source",
        "candle_quality",
    ] = "warning"

    multi_source_mask = quality_df["consensus_quality"] == "multi_source"

    quality_df.loc[
        multi_source_mask
        & (
            quality_df["close_diff_pct"]
            <= quality_df["good_close_diff_pct_threshold"]
        ),
        "candle_quality",
    ] = "good"

    quality_df.loc[
        multi_source_mask
        & (
            quality_df["close_diff_pct"]
            > quality_df["good_close_diff_pct_threshold"]
        )
        & (
            quality_df["close_diff_pct"]
            <= quality_df["warning_close_diff_pct_threshold"]
        ),
        "candle_quality",
    ] = "warning"

    quality_df.loc[
        multi_source_mask
        & (
            quality_df["close_diff_pct"]
            > quality_df["warning_close_diff_pct_threshold"]
        ),
        "candle_quality",
    ] = "bad"

    return quality_df.drop(
        columns=[
            "good_close_diff_pct_threshold",
            "warning_close_diff_pct_threshold",
            "outlier_abs_return_pct_threshold",
        ],
        errors="ignore",
    )


def add_outlier_flag(
    silver_ohlcv_df: pd.DataFrame,
    *,
    thresholds_df: pd.DataFrame | None = None,
    config_dir: Path = Path("config"),
) -> pd.DataFrame:
    if thresholds_df is None:
        thresholds_df = load_silver_quality_thresholds(
            config_dir=config_dir,
        )

    outlier_df = _attach_quality_thresholds(
        silver_ohlcv_df=silver_ohlcv_df,
        thresholds_df=thresholds_df,
    )

    outlier_df = outlier_df.sort_values(
        [
            "asset",
            "timestamp",
        ]
    ).copy()

    outlier_df["return_pct"] = (
        outlier_df
        .groupby("asset")["close"]
        .pct_change()
        * 100
    )

    outlier_df["abs_return_pct"] = outlier_df["return_pct"].abs()

    outlier_df["is_outlier"] = (
        outlier_df["is_generated"]
        & outlier_df["abs_return_pct"].notna()
        & (
            outlier_df["abs_return_pct"]
            > outlier_df["outlier_abs_return_pct_threshold"]
        )
    )

    return outlier_df.drop(
        columns=[
            "good_close_diff_pct_threshold",
            "warning_close_diff_pct_threshold",
            "outlier_abs_return_pct_threshold",
        ],
        errors="ignore",
    )


def build_silver_ohlcv_from_generated(
    generated_df: pd.DataFrame,
    *,
    thresholds_df: pd.DataFrame | None = None,
    config_dir: Path = Path("config"),
) -> pd.DataFrame:
    silver_ohlcv_df = generated_df.copy()

    silver_ohlcv_df = silver_ohlcv_df.rename(
        columns={
            "time": "timestamp",
            "generated_open": "open",
            "generated_high": "high",
            "generated_low": "low",
            "generated_close": "close",
            "generated_volume": "volume",
        }
    )

    silver_ohlcv_df = add_candle_quality_flag(
        silver_ohlcv_df,
        thresholds_df=thresholds_df,
        config_dir=config_dir,
    )

    silver_ohlcv_df = add_outlier_flag(
        silver_ohlcv_df,
        thresholds_df=thresholds_df,
        config_dir=config_dir,
    )

    silver_ohlcv_df = add_quality_status(
        silver_ohlcv_df,
    )

    available_columns = [
        column
        for column in SILVER_OHLCV_COLUMNS
        if column in silver_ohlcv_df.columns
    ]

    return (
        silver_ohlcv_df[available_columns]
        .sort_values(
            [
                "asset",
                "timestamp",
            ]
        )
        .reset_index(drop=True)
    )


def build_silver_ohlcv_summary(
    silver_ohlcv_df: pd.DataFrame,
) -> pd.DataFrame:
    return (
        silver_ohlcv_df
        .groupby(
            [
                "asset",
                "consensus_quality",
                "candle_quality",
                "is_outlier",
                "quality_status",
            ],
            dropna=False,
        )
        .agg(
            rows=("timestamp", "count"),
            generated_rows=("is_generated", "sum"),
            start=("timestamp", "min"),
            end=("timestamp", "max"),
            avg_broker_count=("broker_count", "mean"),
            max_close_diff_pct=("close_diff_pct", "max"),
            max_abs_return_pct=("abs_return_pct", "max"),
        )
        .reset_index()
    )


def add_quality_status(
    silver_ohlcv_df: pd.DataFrame,
) -> pd.DataFrame:
    quality_df = silver_ohlcv_df.copy()

    quality_df["quality_status"] = "good"

    quality_df.loc[
        quality_df["candle_quality"] == "warning",
        "quality_status",
    ] = "warning"

    quality_df.loc[
        (quality_df["candle_quality"] == "bad")
        | (quality_df["is_outlier"]),
        "quality_status",
    ] = "bad"

    quality_df.loc[
        ~quality_df["is_generated"],
        "quality_status",
    ] = "missing"

    return quality_df
