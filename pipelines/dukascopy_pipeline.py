from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient

from metadata.manifest import build_manifest, write_manifest
from metadata.success_marker import write_success_marker
from paths.data_paths import (
    build_dukascopy_bronze_paths,
    build_dukascopy_raw_tick_paths,
)
from planner.download_plan import DownloadPlanItem
from planner.time_windows import TimeWindow
from providers.dukascopy import (
    DukascopyDownloadResult,
    DukascopyProvider,
    DukascopyRequest,
)
from transformers.dukascopy_ohlcv import transform_dukascopy_ticks_to_ohlcv
from transformers.dukascopy_ticks import decode_dukascopy_downloads
from uploaders.azure_blob import blob_exists, upload_file
from validators.dukascopy_ticks import validate_dukascopy_ticks
from writers.dukascopy_raw_parquet import write_dukascopy_raw_ticks_to_parquet
from writers.ohlcv_parquet import write_ohlcv_to_parquet


@dataclass(frozen=True)
class DukascopyPipelineResult:
    status: str
    broker: str
    asset: str
    broker_symbol: str
    year: int
    month: int
    message: str
    errors: list[dict]


def download_dukascopy_hour_with_retry(
    *,
    provider: DukascopyProvider,
    request: DukascopyRequest,
    output_dir: Path,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
) -> tuple[DukascopyDownloadResult | None, dict | None]:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            result = provider.download(
                request=request,
                output_dir=output_dir,
            )

            return result, None

        except Exception as error:
            last_error = error

            if attempt < max_attempts:
                time.sleep(retry_sleep_sec)

    return None, {
        "hour_start": request.hour_start,
        "symbol": request.symbol,
        "attempts": max_attempts,
        "error": str(last_error),
    }


def download_dukascopy_day(
    *,
    symbol: str,
    day_start: datetime,
    output_dir: Path,
    timeout_sec: int = 10,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
    print_progress: bool = False,
) -> tuple[list[DukascopyDownloadResult], list[dict]]:
    provider = DukascopyProvider(timeout_sec=timeout_sec)

    downloads: list[DukascopyDownloadResult] = []
    errors: list[dict] = []

    for hour in range(24):
        hour_start = day_start + timedelta(hours=hour)

        if print_progress:
            print(f"  {symbol} {hour_start:%Y-%m-%d %H}:00 downloading...")

        request = DukascopyRequest(
            symbol=symbol,
            hour_start=hour_start,
        )

        result, error = download_dukascopy_hour_with_retry(
            provider=provider,
            request=request,
            output_dir=output_dir,
            max_attempts=max_attempts,
            retry_sleep_sec=retry_sleep_sec,
        )

        if result is not None:
            downloads.append(result)

            if print_progress:
                print(
                    f"  {symbol} {hour_start:%Y-%m-%d %H}:00 "
                    f"ok bytes={result.file.stat().st_size}"
                )

        if error is not None:
            errors.append(error)

            if print_progress:
                print(
                    f"  {symbol} {hour_start:%Y-%m-%d %H}:00 "
                    f"error={error['error']}"
                )

    return downloads, errors



def build_dukascopy_day_raw_ticks(
    *,
    downloads: list[DukascopyDownloadResult],
    price_scale: int,
) -> pd.DataFrame:
    return decode_dukascopy_downloads(
        downloads=downloads,
        price_scale=price_scale,
    )


def build_dukascopy_raw_ticks_for_window(
    *,
    symbol: str,
    window: TimeWindow,
    staging_dir: Path,
    price_scale: int,
    timeout_sec: int = 10,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
    print_progress: bool = True,
) -> tuple[pd.DataFrame, list[dict]]:
    current_day = datetime(
        window.start_date.year,
        window.start_date.month,
        window.start_date.day,
        tzinfo=timezone.utc,
    )

    end_day = datetime(
        window.end_date.year,
        window.end_date.month,
        window.end_date.day,
        tzinfo=timezone.utc,
    )

    all_tick_frames: list[pd.DataFrame] = []
    all_errors: list[dict] = []

    while current_day <= end_day:
        if print_progress:
            print(f"Downloading Dukascopy day: {symbol} {current_day.date()}")

        downloads, errors = download_dukascopy_day(
            symbol=symbol,
            day_start=current_day,
            output_dir=staging_dir,
            timeout_sec=timeout_sec,
            max_attempts=max_attempts,
            retry_sleep_sec=retry_sleep_sec,
            print_progress=print_progress,
        )

        all_errors.extend(errors)

        day_ticks_df = build_dukascopy_day_raw_ticks(
            downloads=downloads,
            price_scale=price_scale,
        )

        if not day_ticks_df.empty:
            all_tick_frames.append(day_ticks_df)

        if print_progress:
            print(
                f"Done {current_day.date()}: "
                f"downloads={len(downloads)}, "
                f"errors={len(errors)}, "
                f"ticks={len(day_ticks_df)}"
            )

        current_day += timedelta(days=1)

    if not all_tick_frames:
        return (
            pd.DataFrame(
                columns=["timestamp", "bid", "ask", "bid_volume", "ask_volume"]
            ),
            all_errors,
        )

    ticks_df = (
        pd.concat(all_tick_frames, ignore_index=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    return ticks_df, all_errors


def write_and_upload_dukascopy_raw_ticks(
    *,
    ticks_df: pd.DataFrame,
    local_raw_path: Path,
    raw_blob_name: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    overwrite: bool = True,
) -> Path:
    raw_path = write_dukascopy_raw_ticks_to_parquet(
        df=ticks_df,
        output_path=local_raw_path,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=raw_path,
        blob_name=raw_blob_name,
        overwrite=overwrite,
    )

    return raw_path


def run_dukascopy_plan_item(
    *,
    item: DownloadPlanItem,
    interval: str,
    price_scale: int,
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    timeout_sec: int = 10,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
    overwrite: bool = True,
    print_progress: bool = True,
) -> DukascopyPipelineResult:
    year = item.window.start_date.year
    month = item.window.start_date.month
    month_text = f"{month:02d}"

    raw_paths = build_dukascopy_raw_tick_paths(
        broker=item.broker,
        asset=item.asset,
        broker_symbol=item.broker_symbol,
        year=year,
        month=month,
        data_dir=data_dir,
    )

    bronze_paths = build_dukascopy_bronze_paths(
        broker=item.broker,
        asset=item.asset,
        broker_symbol=item.broker_symbol,
        interval=interval,
        year=year,
        month=month,
        data_dir=data_dir,
    )

    if blob_exists(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_name=bronze_paths.success_blob_name,
    ):
        return DukascopyPipelineResult(
            status="skipped",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="_SUCCESS already exists in Azure.",
            errors=[],
        )

    staging_dir = (
        data_dir
        / "staging"
        / item.broker
        / item.asset.lower()
        / str(year)
        / month_text
    )

    ticks_df, errors = build_dukascopy_raw_ticks_for_window(
        symbol=item.broker_symbol,
        window=item.window,
        staging_dir=staging_dir,
        price_scale=price_scale,
        timeout_sec=timeout_sec,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
        print_progress=print_progress,
    )

    if ticks_df.empty:
        return DukascopyPipelineResult(
            status="failed",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="No tick data was built.",
            errors=errors,
        )

    raw_path = write_and_upload_dukascopy_raw_ticks(
        ticks_df=ticks_df,
        local_raw_path=raw_paths.local_raw_tick_file,
        raw_blob_name=raw_paths.raw_tick_blob_name,
        blob_service_client=blob_service_client,
        container_name=container_name,
        overwrite=overwrite,
    )

    validation_result = validate_dukascopy_ticks(ticks_df)

    ohlcv_df = transform_dukascopy_ticks_to_ohlcv(
        ticks_df=ticks_df,
        interval="1min",
    )

    bronze_path = write_ohlcv_to_parquet(
        df=ohlcv_df,
        output_path=bronze_paths.local_bronze_parquet_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=bronze_path,
        blob_name=bronze_paths.bronze_parquet_blob_name,
        overwrite=overwrite,
    )

    manifest = build_manifest(
        provider=item.broker,
        symbol=item.broker_symbol,
        interval=interval,
        year=year,
        month=month,
        source_url="multiple Dukascopy hourly .bi5 files",
        raw_file_path=raw_path,
        parquet_file_path=bronze_path,
        validation_result=validation_result,
        ohlcv_df=ohlcv_df,
    )

    manifest_path = write_manifest(
        manifest=manifest,
        output_path=bronze_paths.local_manifest_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=manifest_path,
        blob_name=bronze_paths.manifest_blob_name,
        overwrite=overwrite,
    )

    success_path = write_success_marker(
        output_path=bronze_paths.local_success_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=success_path,
        blob_name=bronze_paths.success_blob_name,
        overwrite=overwrite,
    )

    return DukascopyPipelineResult(
        status="uploaded",
        broker=item.broker,
        asset=item.asset,
        broker_symbol=item.broker_symbol,
        year=year,
        month=month,
        message="Raw ticks and bronze OHLCV uploaded successfully.",
        errors=errors,
    )


def run_dukascopy_plan(
    *,
    plan: list[DownloadPlanItem],
    interval: str,
    price_scale_by_asset: dict[str, int],
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    timeout_sec: int = 10,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
    overwrite: bool = True,
    print_progress: bool = True,
) -> list[DukascopyPipelineResult]:
    results: list[DukascopyPipelineResult] = []

    for index, item in enumerate(plan):
        price_scale = price_scale_by_asset.get(item.asset)

        if price_scale is None:
            results.append(
                DukascopyPipelineResult(
                    status="failed",
                    broker=item.broker,
                    asset=item.asset,
                    broker_symbol=item.broker_symbol,
                    year=item.window.start_date.year,
                    month=item.window.start_date.month,
                    message=f"Missing price scale for asset: {item.asset}",
                    errors=[],
                )
            )
            continue

        result = run_dukascopy_plan_item(
            item=item,
            interval=interval,
            price_scale=price_scale,
            blob_service_client=blob_service_client,
            container_name=container_name,
            data_dir=data_dir,
            timeout_sec=timeout_sec,
            max_attempts=max_attempts,
            retry_sleep_sec=retry_sleep_sec,
            overwrite=overwrite,
            print_progress=print_progress,
        )

        results.append(result)

        is_last_item = index == len(plan) - 1

        if not is_last_item and item.rate_limit_sleep_sec > 0:
            time.sleep(item.rate_limit_sleep_sec)

    return results
