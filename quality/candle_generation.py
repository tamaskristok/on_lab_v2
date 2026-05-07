from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from quality.calendar_alignment import load_calendar
from quality.data_loader import load_bronze_ohlcv


OPTIONAL_CALENDAR_COLUMNS = [
    "year",
    "month",
    "expected",
    "calendar_source",
    "calendar_timezone",
    "session_ref_date",
]

BROKER_VALUE_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_rows",
]

GENERATED_CANDLE_COLUMNS = [
    "time",
    "asset",
    "generated_open",
    "generated_high",
    "generated_low",
    "generated_close",
    "generated_volume",
    "broker_count",
    "consensus_quality",
    "generation_method_id",
    "generation_method",
    "is_generated",
    "close_diff",
    "close_diff_pct",
]


def _calendar_columns(
    *,
    calendar_df: pd.DataFrame,
) -> list[str]:
    return [
        "time",
        "asset",
    ] + [
        column
        for column in OPTIONAL_CALENDAR_COLUMNS
        if column in calendar_df.columns
    ]


def _build_calendar_key_df(
    *,
    calendar_df: pd.DataFrame,
) -> pd.DataFrame:
    calendar_filter_df = calendar_df.copy()

    calendar_filter_df["timestamp"] = pd.to_datetime(
        calendar_filter_df["timestamp"],
        utc=True,
        errors="coerce",
    )
    calendar_filter_df["time"] = calendar_filter_df["timestamp"].dt.floor("min")
    calendar_filter_df["asset"] = calendar_filter_df["asset"].str.upper()

    calendar_columns = _calendar_columns(
        calendar_df=calendar_filter_df,
    )

    return (
        calendar_filter_df[calendar_columns]
        .dropna(subset=["time", "asset"])
        .drop_duplicates(subset=["time", "asset"])
        .copy()
    )


def filter_bronze_to_calendar(
    *,
    bronze_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> pd.DataFrame:
    source_df = bronze_df.copy()
    calendar_key_df = _build_calendar_key_df(
        calendar_df=calendar_df,
    )

    if "broker" in source_df.columns:
        source_df = source_df[source_df["broker"].notna()].copy()

    source_df["timestamp"] = pd.to_datetime(
        source_df["timestamp"],
        utc=True,
        errors="coerce",
    )
    source_df["time"] = source_df["timestamp"].dt.floor("min")
    source_df["asset"] = source_df["asset"].str.upper()

    return source_df.merge(
        calendar_key_df[["time", "asset"]],
        on=["time", "asset"],
        how="inner",
    )


def build_broker_minute_table(
    *,
    bronze_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> pd.DataFrame:
    filtered_df = filter_bronze_to_calendar(
        bronze_df=bronze_df,
        calendar_df=calendar_df,
    )

    if filtered_df.empty:
        return pd.DataFrame(
            columns=[
                "time",
                "asset",
                "broker",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source_rows",
            ]
        )

    return (
        filtered_df
        .sort_values(["asset", "broker", "time", "timestamp"])
        .groupby(["time", "asset", "broker"], as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_rows=("timestamp", "count"),
        )
    )


def build_broker_wide_table(
    *,
    bronze_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> pd.DataFrame:
    calendar_key_df = _build_calendar_key_df(
        calendar_df=calendar_df,
    )

    minute_broker_df = build_broker_minute_table(
        bronze_df=bronze_df,
        calendar_df=calendar_df,
    )

    if minute_broker_df.empty:
        quality_wide_df = calendar_key_df.copy()
        quality_wide_df["generated_candle_slot"] = True
        quality_wide_df["broker_count"] = 0
        quality_wide_df["close_min"] = pd.NA
        quality_wide_df["close_max"] = pd.NA
        quality_wide_df["close_diff"] = pd.NA
        quality_wide_df["close_diff_pct"] = pd.NA
        return quality_wide_df

    wide_df = minute_broker_df.pivot(
        index=["time", "asset"],
        columns="broker",
        values=BROKER_VALUE_FIELDS,
    )

    wide_df.columns = [
        f"{broker}_{field}"
        for field, broker in wide_df.columns
    ]
    wide_df = wide_df.reset_index()

    broker_close_columns = [
        column
        for column in wide_df.columns
        if column.endswith("_close")
    ]

    wide_df["broker_count"] = wide_df[broker_close_columns].notna().sum(axis=1)
    wide_df["close_min"] = wide_df[broker_close_columns].min(axis=1)
    wide_df["close_max"] = wide_df[broker_close_columns].max(axis=1)
    wide_df["close_diff"] = wide_df["close_max"] - wide_df["close_min"]
    wide_df["close_diff_pct"] = (
        wide_df["close_diff"] / wide_df["close_min"] * 100
    )

    quality_wide_df = calendar_key_df.merge(
        wide_df,
        on=["time", "asset"],
        how="left",
    )

    quality_wide_df["generated_candle_slot"] = True

    return (
        quality_wide_df
        .sort_values(["asset", "time"])
        .reset_index(drop=True)
    )


def _broker_columns(
    *,
    df: pd.DataFrame,
    field: str,
) -> list[str]:
    return [
        column
        for column in df.columns
        if column.endswith(f"_{field}")
    ]


def _extract_broker_names(
    df: pd.DataFrame,
) -> list[str]:
    broker_names = []

    for column in df.columns:
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


def _build_preferred_broker_order(
    quality_wide_df: pd.DataFrame,
) -> dict[str, list[str]]:
    broker_names = _extract_broker_names(
        quality_wide_df,
    )

    preferred_order_by_asset: dict[str, list[str]] = {}

    for asset, asset_df in quality_wide_df.groupby("asset"):
        close_columns = [
            f"{broker_name}_close"
            for broker_name in broker_names
            if f"{broker_name}_close" in asset_df.columns
        ]

        if not close_columns:
            preferred_order_by_asset[asset] = []
            continue

        close_reference = asset_df[close_columns].median(
            axis=1,
            skipna=True,
        )

        records = []

        for broker in broker_names:
            close_column = f"{broker}_close"

            if close_column not in asset_df.columns:
                continue

            broker_present_mask = asset_df[close_column].notna()
            broker_rows = int(broker_present_mask.sum())

            if broker_rows == 0:
                continue

            coverage_ratio = broker_rows / len(asset_df)
            broker_close = asset_df[close_column]

            abs_diff_pct = (
                (broker_close - close_reference).abs()
                / close_reference
                * 100
            )

            records.append(
                {
                    "broker": broker,
                    "coverage_ratio": coverage_ratio,
                    "mean_abs_diff_pct": abs_diff_pct.mean(),
                }
            )

        ranking_df = pd.DataFrame(records)

        if ranking_df.empty:
            preferred_order_by_asset[asset] = []
            continue

        ranking_df = ranking_df.sort_values(
            [
                "coverage_ratio",
                "mean_abs_diff_pct",
                "broker",
            ],
            ascending=[
                False,
                True,
                True,
            ],
        )

        preferred_order_by_asset[asset] = ranking_df["broker"].tolist()

    return preferred_order_by_asset


def _apply_preferred_broker_ohlc(
    quality_wide_df: pd.DataFrame,
) -> pd.DataFrame:
    generated_df = quality_wide_df.copy()

    preferred_order_by_asset = _build_preferred_broker_order(
        quality_wide_df,
    )

    generated_df["generated_open"] = np.nan
    generated_df["generated_high"] = np.nan
    generated_df["generated_low"] = np.nan
    generated_df["generated_close"] = np.nan
    generated_df["generated_volume"] = pd.NA
    generated_df["selected_broker"] = pd.NA

    for asset, brokers in preferred_order_by_asset.items():
        if not brokers:
            continue

        asset_mask = generated_df["asset"] == asset
        asset_index = generated_df.index[asset_mask]

        if len(asset_index) == 0:
            continue

        for field in [
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]:
            field_columns = [
                f"{broker}_{field}"
                for broker in brokers
                if f"{broker}_{field}" in generated_df.columns
            ]

            if not field_columns:
                continue

            generated_df.loc[
                asset_index,
                f"generated_{field}",
            ] = (
                generated_df
                .loc[asset_index, field_columns]
                .bfill(axis=1)
                .iloc[:, 0]
            )

        close_columns = [
            f"{broker}_close"
            for broker in brokers
            if f"{broker}_close" in generated_df.columns
        ]

        if not close_columns:
            continue

        close_presence_df = generated_df.loc[
            asset_index,
            close_columns,
        ].notna()

        selected_broker_series = close_presence_df.idxmax(axis=1)
        has_selected_broker = close_presence_df.any(axis=1)

        selected_broker_series = selected_broker_series.where(
            has_selected_broker,
            pd.NA,
        )

        selected_broker_series = selected_broker_series.str.removesuffix(
            "_close"
        )

        generated_df.loc[
            asset_index,
            "selected_broker",
        ] = selected_broker_series

    return generated_df


def generate_candles_from_brokers(
    quality_wide_df: pd.DataFrame,
    *,
    method: int = 0,
) -> pd.DataFrame:
    generated_df = quality_wide_df.copy()

    open_columns = _broker_columns(
        df=generated_df,
        field="open",
    )
    high_columns = _broker_columns(
        df=generated_df,
        field="high",
    )
    low_columns = _broker_columns(
        df=generated_df,
        field="low",
    )
    close_columns = _broker_columns(
        df=generated_df,
        field="close",
    )

    if method == 0:
        method_name = "median_ohlc"

        generated_df["generated_open"] = generated_df[open_columns].median(
            axis=1,
            skipna=True,
        )
        generated_df["generated_high"] = generated_df[high_columns].median(
            axis=1,
            skipna=True,
        )
        generated_df["generated_low"] = generated_df[low_columns].median(
            axis=1,
            skipna=True,
        )
        generated_df["generated_close"] = generated_df[close_columns].median(
            axis=1,
            skipna=True,
        )
        generated_df["generated_volume"] = pd.NA
    elif method == 1:
        method_name = "preferred_broker_ohlc"

        generated_df = _apply_preferred_broker_ohlc(
            generated_df,
        )
    else:
        raise ValueError(f"Unsupported generation method: {method}")

    generated_df["generation_method_id"] = method
    generated_df["generation_method"] = method_name

    generated_df["consensus_quality"] = "missing"
    generated_df.loc[
        generated_df["broker_count"] == 1,
        "consensus_quality",
    ] = "single_source"
    generated_df.loc[
        generated_df["broker_count"] >= 2,
        "consensus_quality",
    ] = "multi_source"

    generated_df["is_generated"] = generated_df["generated_close"].notna()

    return generated_df


def build_generated_candles_from_dataframes(
    *,
    bronze_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
    method: int = 0,
) -> pd.DataFrame:
    quality_wide_df = build_broker_wide_table(
        bronze_df=bronze_df,
        calendar_df=calendar_df,
    )

    return generate_candles_from_brokers(
        quality_wide_df,
        method=method,
    )


def build_generated_candles(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | str | None = None,
    assets: list[str] | str | None = None,
    interval: str = "1m",
    method: int = 0,
    source: str = "azure",
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    data_dir: Path = Path("data"),
    print_progress: bool = False,
) -> pd.DataFrame:
    bronze_df = load_bronze_ohlcv(
        start_month=start_month,
        end_month=end_month,
        brokers=brokers,
        assets=assets,
        interval=interval,
        source=source,
        config_dir=config_dir,
        env_path=env_path,
        container_name=container_name,
        data_dir=data_dir,
        print_progress=print_progress,
    )

    calendar_df = load_calendar(
        start_month=start_month,
        end_month=end_month,
        assets=assets,
        interval=interval,
        source=source,
        env_path=env_path,
        container_name=container_name,
        data_dir=data_dir,
        print_progress=print_progress,
    )

    return build_generated_candles_from_dataframes(
        bronze_df=bronze_df,
        calendar_df=calendar_df,
        method=method,
    )
