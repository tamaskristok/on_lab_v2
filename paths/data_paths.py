from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from planner.download_plan import DownloadPlanItem


@dataclass(frozen=True)
class DataPaths:
    local_raw_file: Path
    local_bronze_parquet_file: Path
    local_manifest_file: Path
    local_success_file: Path

    raw_blob_name: str
    bronze_parquet_blob_name: str
    manifest_blob_name: str
    success_blob_name: str


@dataclass(frozen=True)
class DukascopyRawTickPaths:
    local_raw_tick_file: Path
    raw_tick_blob_name: str


@dataclass(frozen=True)
class DukascopyBronzePaths:
    local_bronze_parquet_file: Path
    local_manifest_file: Path
    local_success_file: Path

    bronze_parquet_blob_name: str
    manifest_blob_name: str
    success_blob_name: str


@dataclass(frozen=True)
class SaxoBankPaths:
    local_raw_chart_file: Path
    local_bronze_parquet_file: Path
    local_manifest_file: Path
    local_success_file: Path

    raw_chart_blob_name: str
    bronze_parquet_blob_name: str
    manifest_blob_name: str
    success_blob_name: str


@dataclass(frozen=True)
class InteractiveBrokersPaths:
    local_raw_bars_file: Path
    local_bronze_parquet_file: Path
    local_manifest_file: Path
    local_success_file: Path

    raw_bars_blob_name: str
    bronze_parquet_blob_name: str
    manifest_blob_name: str
    success_blob_name: str


def build_data_paths(
    item: DownloadPlanItem,
    interval: str,
    data_dir: Path = Path("data"),
) -> DataPaths:
    year = item.window.start_date.year
    month = item.window.start_date.month
    month_text = f"{month:02d}"

    asset_folder = item.asset.lower()

    file_stem = f"{item.broker_symbol}-{interval}-{year}-{month_text}"

    local_raw_file = (
        data_dir
        / "raw"
        / item.broker
        / asset_folder
        / str(year)
        / month_text
        / f"{file_stem}.csv"
    )

    local_bronze_dir = (
        data_dir
        / "bronze"
        / item.broker
        / asset_folder
        / str(year)
        / month_text
    )

    local_bronze_parquet_file = local_bronze_dir / f"{file_stem}.parquet"
    local_manifest_file = local_bronze_dir / "_MANIFEST.json"
    local_success_file = local_bronze_dir / "_SUCCESS"

    raw_blob_name = (
        f"raw/{item.broker}/{asset_folder}/{year}/{month_text}/{file_stem}.csv"
    )

    bronze_parquet_blob_name = (
        f"bronze/{item.broker}/{asset_folder}/{year}/{month_text}/{file_stem}.parquet"
    )

    manifest_blob_name = (
        f"bronze/{item.broker}/{asset_folder}/{year}/{month_text}/_MANIFEST.json"
    )

    success_blob_name = (
        f"bronze/{item.broker}/{asset_folder}/{year}/{month_text}/_SUCCESS"
    )

    return DataPaths(
        local_raw_file=local_raw_file,
        local_bronze_parquet_file=local_bronze_parquet_file,
        local_manifest_file=local_manifest_file,
        local_success_file=local_success_file,
        raw_blob_name=raw_blob_name,
        bronze_parquet_blob_name=bronze_parquet_blob_name,
        manifest_blob_name=manifest_blob_name,
        success_blob_name=success_blob_name,
    )


def build_dukascopy_raw_tick_paths(
    *,
    broker: str,
    asset: str,
    broker_symbol: str,
    year: int,
    month: int,
    data_dir: Path = Path("data"),
) -> DukascopyRawTickPaths:
    month_text = f"{month:02d}"
    asset_folder = asset.lower()

    file_name = f"{broker_symbol}-ticks-{year}-{month_text}.parquet"

    local_raw_tick_file = (
        data_dir
        / "raw"
        / broker
        / asset_folder
        / str(year)
        / month_text
        / file_name
    )

    raw_tick_blob_name = (
        f"raw/{broker}/{asset_folder}/{year}/{month_text}/{file_name}"
    )

    return DukascopyRawTickPaths(
        local_raw_tick_file=local_raw_tick_file,
        raw_tick_blob_name=raw_tick_blob_name,
    )


def build_dukascopy_bronze_paths(
    *,
    broker: str,
    asset: str,
    broker_symbol: str,
    interval: str,
    year: int,
    month: int,
    data_dir: Path = Path("data"),
) -> DukascopyBronzePaths:
    month_text = f"{month:02d}"
    asset_folder = asset.lower()

    file_name = f"{broker_symbol}-{interval}-{year}-{month_text}.parquet"

    local_bronze_dir = (
        data_dir
        / "bronze"
        / broker
        / asset_folder
        / str(year)
        / month_text
    )

    local_bronze_parquet_file = local_bronze_dir / file_name
    local_manifest_file = local_bronze_dir / "_MANIFEST.json"
    local_success_file = local_bronze_dir / "_SUCCESS"

    bronze_parquet_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/{file_name}"
    )
    manifest_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/_MANIFEST.json"
    )
    success_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/_SUCCESS"
    )

    return DukascopyBronzePaths(
        local_bronze_parquet_file=local_bronze_parquet_file,
        local_manifest_file=local_manifest_file,
        local_success_file=local_success_file,
        bronze_parquet_blob_name=bronze_parquet_blob_name,
        manifest_blob_name=manifest_blob_name,
        success_blob_name=success_blob_name,
    )


def build_saxo_bank_paths(
    *,
    broker: str,
    asset: str,
    broker_symbol: str,
    interval: str,
    year: int,
    month: int,
    data_dir: Path = Path("data"),
) -> SaxoBankPaths:
    month_text = f"{month:02d}"
    asset_folder = asset.lower()

    raw_file_name = f"{broker_symbol}-chart-{year}-{month_text}.parquet"
    bronze_file_name = f"{broker_symbol}-{interval}-{year}-{month_text}.parquet"

    local_raw_chart_file = (
        data_dir
        / "raw"
        / broker
        / asset_folder
        / str(year)
        / month_text
        / raw_file_name
    )

    local_bronze_dir = (
        data_dir
        / "bronze"
        / broker
        / asset_folder
        / str(year)
        / month_text
    )

    local_bronze_parquet_file = local_bronze_dir / bronze_file_name
    local_manifest_file = local_bronze_dir / "_MANIFEST.json"
    local_success_file = local_bronze_dir / "_SUCCESS"

    raw_chart_blob_name = (
        f"raw/{broker}/{asset_folder}/{year}/{month_text}/{raw_file_name}"
    )
    bronze_parquet_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/{bronze_file_name}"
    )
    manifest_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/_MANIFEST.json"
    )
    success_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/_SUCCESS"
    )

    return SaxoBankPaths(
        local_raw_chart_file=local_raw_chart_file,
        local_bronze_parquet_file=local_bronze_parquet_file,
        local_manifest_file=local_manifest_file,
        local_success_file=local_success_file,
        raw_chart_blob_name=raw_chart_blob_name,
        bronze_parquet_blob_name=bronze_parquet_blob_name,
        manifest_blob_name=manifest_blob_name,
        success_blob_name=success_blob_name,
    )


def build_interactive_brokers_paths(
    *,
    broker: str,
    asset: str,
    broker_symbol: str,
    interval: str,
    year: int,
    month: int,
    data_dir: Path = Path("data"),
) -> InteractiveBrokersPaths:
    month_text = f"{month:02d}"
    asset_folder = asset.lower()

    raw_file_name = f"{broker_symbol}-bars-{year}-{month_text}.parquet"
    bronze_file_name = f"{broker_symbol}-{interval}-{year}-{month_text}.parquet"

    local_raw_bars_file = (
        data_dir
        / "raw"
        / broker
        / asset_folder
        / str(year)
        / month_text
        / raw_file_name
    )

    local_bronze_dir = (
        data_dir
        / "bronze"
        / broker
        / asset_folder
        / str(year)
        / month_text
    )

    local_bronze_parquet_file = local_bronze_dir / bronze_file_name
    local_manifest_file = local_bronze_dir / "_MANIFEST.json"
    local_success_file = local_bronze_dir / "_SUCCESS"

    raw_bars_blob_name = (
        f"raw/{broker}/{asset_folder}/{year}/{month_text}/{raw_file_name}"
    )
    bronze_parquet_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/{bronze_file_name}"
    )
    manifest_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/_MANIFEST.json"
    )
    success_blob_name = (
        f"bronze/{broker}/{asset_folder}/{year}/{month_text}/_SUCCESS"
    )

    return InteractiveBrokersPaths(
        local_raw_bars_file=local_raw_bars_file,
        local_bronze_parquet_file=local_bronze_parquet_file,
        local_manifest_file=local_manifest_file,
        local_success_file=local_success_file,
        raw_bars_blob_name=raw_bars_blob_name,
        bronze_parquet_blob_name=bronze_parquet_blob_name,
        manifest_blob_name=manifest_blob_name,
        success_blob_name=success_blob_name,
    )
