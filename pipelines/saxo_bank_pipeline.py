from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient

from metadata.manifest import build_manifest, write_manifest
from metadata.success_marker import write_success_marker
from paths.data_paths import build_saxo_bank_paths
from planner.download_plan import DownloadPlanItem
from planner.saxo_bank_half_day_windows import generate_saxo_bank_half_day_windows
from providers.saxo_bank import SaxoBankProvider, SaxoChartRequest
from transformers.saxo_bank_ohlcv import transform_saxo_bank_chart_to_ohlcv
from uploaders.azure_blob import blob_exists, upload_file
from validators.saxo_bank_chart import validate_saxo_bank_ohlcv
from writers.ohlcv_parquet import write_ohlcv_to_parquet
from writers.saxo_bank_raw_parquet import write_saxo_bank_raw_chart_to_parquet


DEFAULT_SAXO_BANK_BASE_URL = "https://gateway.saxobank.com/sim/openapi"
DEFAULT_SAXO_BANK_TIMEOUT_SEC = 30
DEFAULT_SAXO_BANK_HORIZON = 1
DEFAULT_SAXO_BANK_MAX_ATTEMPTS = 2
DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC = 10
DEFAULT_SAXO_BANK_REQUEST_SLEEP_SEC = 1


@dataclass(frozen=True)
class SaxoBankInstrument:
    asset: str
    symbol: str
    uic: int
    asset_type: str
    volume_policy: str


@dataclass(frozen=True)
class SaxoBankPipelineResult:
    status: str
    broker: str
    asset: str
    broker_symbol: str
    year: int
    month: int
    message: str
    errors: list[dict]


def build_saxo_bank_instrument_by_asset(
    saxo_bank_instruments_df: pd.DataFrame,
) -> dict[str, SaxoBankInstrument]:
    instruments: dict[str, SaxoBankInstrument] = {}

    for _, row in saxo_bank_instruments_df.iterrows():
        instrument = SaxoBankInstrument(
            asset=str(row["asset"]).strip(),
            symbol=str(row["symbol"]).strip(),
            uic=int(row["uic"]),
            asset_type=str(row["asset_type"]).strip(),
            volume_policy=str(row["volume_policy"]).strip(),
        )

        instruments[instrument.asset] = instrument

    return instruments


def download_saxo_bank_chart_with_retry(
    *,
    provider: SaxoBankProvider,
    request: SaxoChartRequest,
    max_attempts: int = DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
) -> tuple[list[dict], dict | None]:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            result = provider.download_chart(request)
            return result.data, None

        except Exception as error:
            last_error = error

            if attempt < max_attempts:
                time.sleep(retry_sleep_sec)

    return [], {
        "start_time": request.start_time,
        "uic": request.uic,
        "asset_type": request.asset_type,
        "attempts": max_attempts,
        "error": str(last_error),
    }


def download_saxo_bank_month_chart_rows(
    *,
    provider: SaxoBankProvider,
    item: DownloadPlanItem,
    instrument: SaxoBankInstrument,
    horizon: int = DEFAULT_SAXO_BANK_HORIZON,
    max_attempts: int = DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_SAXO_BANK_REQUEST_SLEEP_SEC,
    print_progress: bool = True,
) -> tuple[list[dict], list[dict]]:
    half_day_windows = generate_saxo_bank_half_day_windows(item.window)

    all_rows: list[dict] = []
    errors: list[dict] = []

    for half_day_window in half_day_windows:
        if print_progress:
            print(
                f"  {item.asset} {half_day_window.start_time:%Y-%m-%d %H:%M} "
                f"downloading..."
            )

        request = SaxoChartRequest(
            uic=instrument.uic,
            asset_type=instrument.asset_type,
            horizon=horizon,
            start_time=half_day_window.start_time,
            count=half_day_window.count,
        )

        rows, error = download_saxo_bank_chart_with_retry(
            provider=provider,
            request=request,
            max_attempts=max_attempts,
            retry_sleep_sec=retry_sleep_sec,
        )

        if rows:
            all_rows.extend(rows)

            if print_progress:
                print(
                    f"  {item.asset} {half_day_window.start_time:%Y-%m-%d %H:%M} "
                    f"ok rows={len(rows)}"
                )

        if error is not None:
            errors.append(error)

            if print_progress:
                print(
                    f"  {item.asset} {half_day_window.start_time:%Y-%m-%d %H:%M} "
                    f"error={error['error']}"
                )

        if request_sleep_sec > 0:
            time.sleep(request_sleep_sec)

    return all_rows, errors


def filter_ohlcv_to_window(
    *,
    ohlcv_df: pd.DataFrame,
    item: DownloadPlanItem,
) -> pd.DataFrame:
    if ohlcv_df.empty:
        return ohlcv_df

    start_timestamp = pd.Timestamp(item.window.start_date, tz="UTC")
    end_timestamp = pd.Timestamp(item.window.end_date, tz="UTC") + pd.Timedelta(days=1)

    return (
        ohlcv_df[
            (ohlcv_df["timestamp"] >= start_timestamp)
            & (ohlcv_df["timestamp"] < end_timestamp)
        ]
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def run_saxo_bank_plan_item(
    *,
    item: DownloadPlanItem,
    interval: str,
    instrument: SaxoBankInstrument,
    access_token: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    base_url: str = DEFAULT_SAXO_BANK_BASE_URL,
    data_dir: Path = Path("data"),
    timeout_sec: int = DEFAULT_SAXO_BANK_TIMEOUT_SEC,
    horizon: int = DEFAULT_SAXO_BANK_HORIZON,
    max_attempts: int = DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_SAXO_BANK_REQUEST_SLEEP_SEC,
    overwrite: bool = True,
    print_progress: bool = True,
) -> SaxoBankPipelineResult:
    year = item.window.start_date.year
    month = item.window.start_date.month

    paths = build_saxo_bank_paths(
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
        blob_name=paths.success_blob_name,
    ):
        return SaxoBankPipelineResult(
            status="skipped",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="_SUCCESS already exists in Azure.",
            errors=[],
        )

    provider = SaxoBankProvider(
        access_token=access_token,
        base_url=base_url,
        timeout_sec=timeout_sec,
    )

    chart_rows, errors = download_saxo_bank_month_chart_rows(
        provider=provider,
        item=item,
        instrument=instrument,
        horizon=horizon,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
        request_sleep_sec=request_sleep_sec,
        print_progress=print_progress,
    )

    if not chart_rows:
        return SaxoBankPipelineResult(
            status="failed",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="No Saxo chart data was downloaded.",
            errors=errors,
        )

    raw_path = write_saxo_bank_raw_chart_to_parquet(
        chart_rows=chart_rows,
        output_path=paths.local_raw_chart_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=raw_path,
        blob_name=paths.raw_chart_blob_name,
        overwrite=overwrite,
    )

    ohlcv_df = transform_saxo_bank_chart_to_ohlcv(chart_rows)

    ohlcv_df = filter_ohlcv_to_window(
        ohlcv_df=ohlcv_df,
        item=item,
    )

    if ohlcv_df.empty:
        return SaxoBankPipelineResult(
            status="failed",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="No Saxo OHLCV data was built.",
            errors=errors,
        )

    validation_result = validate_saxo_bank_ohlcv(ohlcv_df)

    bronze_path = write_ohlcv_to_parquet(
        df=ohlcv_df,
        output_path=paths.local_bronze_parquet_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=bronze_path,
        blob_name=paths.bronze_parquet_blob_name,
        overwrite=overwrite,
    )

    manifest = build_manifest(
        provider=item.broker,
        symbol=item.broker_symbol,
        interval=interval,
        year=year,
        month=month,
        source_url="Saxo Bank chart/v3/charts",
        raw_file_path=raw_path,
        parquet_file_path=bronze_path,
        validation_result=validation_result,
        ohlcv_df=ohlcv_df,
        ingestion_errors=errors,
    )

    manifest_path = write_manifest(
        manifest=manifest,
        output_path=paths.local_manifest_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=manifest_path,
        blob_name=paths.manifest_blob_name,
        overwrite=overwrite,
    )

    success_path = write_success_marker(
        output_path=paths.local_success_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=success_path,
        blob_name=paths.success_blob_name,
        overwrite=overwrite,
    )

    return SaxoBankPipelineResult(
        status="uploaded",
        broker=item.broker,
        asset=item.asset,
        broker_symbol=item.broker_symbol,
        year=year,
        month=month,
        message="Raw chart and bronze OHLCV uploaded successfully.",
        errors=errors,
    )


def run_saxo_bank_plan(
    *,
    plan: list[DownloadPlanItem],
    interval: str,
    instrument_by_asset: dict[str, SaxoBankInstrument],
    access_token: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    base_url: str = DEFAULT_SAXO_BANK_BASE_URL,
    data_dir: Path = Path("data"),
    timeout_sec: int = DEFAULT_SAXO_BANK_TIMEOUT_SEC,
    horizon: int = DEFAULT_SAXO_BANK_HORIZON,
    max_attempts: int = DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_SAXO_BANK_REQUEST_SLEEP_SEC,
    overwrite: bool = True,
    print_progress: bool = True,
) -> list[SaxoBankPipelineResult]:
    results: list[SaxoBankPipelineResult] = []

    for index, item in enumerate(plan):
        instrument = instrument_by_asset.get(item.asset)

        if instrument is None:
            results.append(
                SaxoBankPipelineResult(
                    status="failed",
                    broker=item.broker,
                    asset=item.asset,
                    broker_symbol=item.broker_symbol,
                    year=item.window.start_date.year,
                    month=item.window.start_date.month,
                    message=f"Missing Saxo instrument mapping for asset: {item.asset}",
                    errors=[],
                )
            )
            continue

        result = run_saxo_bank_plan_item(
            item=item,
            interval=interval,
            instrument=instrument,
            access_token=access_token,
            blob_service_client=blob_service_client,
            container_name=container_name,
            base_url=base_url,
            data_dir=data_dir,
            timeout_sec=timeout_sec,
            horizon=horizon,
            max_attempts=max_attempts,
            retry_sleep_sec=retry_sleep_sec,
            request_sleep_sec=request_sleep_sec,
            overwrite=overwrite,
            print_progress=print_progress,
        )

        results.append(result)

        is_last_item = index == len(plan) - 1

        if not is_last_item and item.rate_limit_sleep_sec > 0:
            time.sleep(item.rate_limit_sleep_sec)

    return results
