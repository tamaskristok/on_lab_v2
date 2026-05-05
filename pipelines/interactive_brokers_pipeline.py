from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient

from metadata.manifest import build_manifest, write_manifest
from metadata.success_marker import write_success_marker
from paths.data_paths import build_interactive_brokers_paths
from planner.download_plan import DownloadPlanItem
from planner.interactive_brokers_two_week_windows import (
    generate_interactive_brokers_two_week_windows,
)
from providers.interactive_brokers import (
    InteractiveBrokersHistoricalRequest,
    InteractiveBrokersProvider,
)
from transformers.interactive_brokers_ohlcv import (
    transform_interactive_brokers_bars_to_ohlcv,
)
from uploaders.azure_blob import blob_exists, upload_file
from validators.interactive_brokers_bars import validate_interactive_brokers_ohlcv
from writers.interactive_brokers_raw_parquet import (
    write_interactive_brokers_raw_bars_to_parquet,
)
from writers.ohlcv_parquet import write_ohlcv_to_parquet


DEFAULT_INTERACTIVE_BROKERS_HOST = "host.docker.internal"
DEFAULT_INTERACTIVE_BROKERS_PORT = 7497
DEFAULT_INTERACTIVE_BROKERS_CLIENT_ID = 30
DEFAULT_INTERACTIVE_BROKERS_TIMEOUT_SEC = 10
DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE = "1 min"
DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS = 2
DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC = 10
DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC = 1


@dataclass(frozen=True)
class InteractiveBrokersInstrument:
    asset: str
    symbol: str
    con_id: int
    security_type: str
    exchange: str
    currency: str
    what_to_show: str
    volume_policy: str
    notes: str


@dataclass(frozen=True)
class InteractiveBrokersPipelineResult:
    status: str
    broker: str
    asset: str
    broker_symbol: str
    year: int
    month: int
    message: str
    errors: list[dict]


def build_interactive_brokers_instrument_by_asset(
    interactive_brokers_instruments_df: pd.DataFrame,
) -> dict[str, InteractiveBrokersInstrument]:
    instruments: dict[str, InteractiveBrokersInstrument] = {}

    for _, row in interactive_brokers_instruments_df.iterrows():
        instrument = InteractiveBrokersInstrument(
            asset=str(row["asset"]).strip(),
            symbol=str(row["symbol"]).strip(),
            con_id=int(row["con_id"]),
            security_type=str(row["security_type"]).strip(),
            exchange=str(row["exchange"]).strip(),
            currency=str(row["currency"]).strip(),
            what_to_show=str(row["what_to_show"]).strip(),
            volume_policy=str(row["volume_policy"]).strip(),
            notes=str(row["notes"]).strip(),
        )

        instruments[instrument.asset] = instrument

    return instruments


def download_interactive_brokers_bars_with_retry(
    *,
    provider: InteractiveBrokersProvider,
    request: InteractiveBrokersHistoricalRequest,
    max_attempts: int = DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
) -> tuple[list[dict], dict | None]:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            result = provider.download_historical_bars(request)
            return result.data, None

        except Exception as error:
            last_error = error

            if attempt < max_attempts:
                time.sleep(retry_sleep_sec)

    return [], {
        "end_datetime": request.end_datetime,
        "duration": request.duration,
        "con_id": request.con_id,
        "symbol": request.symbol,
        "security_type": request.security_type,
        "exchange": request.exchange,
        "currency": request.currency,
        "what_to_show": request.what_to_show,
        "attempts": max_attempts,
        "error": str(last_error),
    }


def download_interactive_brokers_month_bars(
    *,
    provider: InteractiveBrokersProvider,
    item: DownloadPlanItem,
    instrument: InteractiveBrokersInstrument,
    bar_size: str = DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    max_attempts: int = DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    print_progress: bool = True,
) -> tuple[list[dict], list[dict]]:
    windows = generate_interactive_brokers_two_week_windows(item.window)

    all_bars: list[dict] = []
    errors: list[dict] = []

    for window in windows:
        if print_progress:
            print(
                f"  {item.asset} {window.end_datetime:%Y-%m-%d %H:%M} "
                f"downloading..."
            )

        request = InteractiveBrokersHistoricalRequest(
            con_id=instrument.con_id,
            symbol=instrument.symbol,
            security_type=instrument.security_type,
            exchange=instrument.exchange,
            currency=instrument.currency,
            end_datetime=window.end_datetime,
            duration=window.duration,
            bar_size=bar_size,
            what_to_show=instrument.what_to_show,
            use_regular_trading_hours=False,
        )

        bars, error = download_interactive_brokers_bars_with_retry(
            provider=provider,
            request=request,
            max_attempts=max_attempts,
            retry_sleep_sec=retry_sleep_sec,
        )

        if bars:
            all_bars.extend(bars)

            if print_progress:
                print(
                    f"  {item.asset} {window.end_datetime:%Y-%m-%d %H:%M} "
                    f"ok rows={len(bars)}"
                )

        if error is not None:
            errors.append(error)

            if print_progress:
                print(
                    f"  {item.asset} {window.end_datetime:%Y-%m-%d %H:%M} "
                    f"error={error['error']}"
                )

        if request_sleep_sec > 0:
            time.sleep(request_sleep_sec)

    return all_bars, errors


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


def run_interactive_brokers_plan_item(
    *,
    item: DownloadPlanItem,
    interval: str,
    instrument: InteractiveBrokersInstrument,
    provider: InteractiveBrokersProvider,
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    bar_size: str = DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    max_attempts: int = DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    overwrite: bool = True,
    print_progress: bool = True,
) -> InteractiveBrokersPipelineResult:
    year = item.window.start_date.year
    month = item.window.start_date.month

    paths = build_interactive_brokers_paths(
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
        return InteractiveBrokersPipelineResult(
            status="skipped",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="_SUCCESS already exists in Azure.",
            errors=[],
        )

    bars, errors = download_interactive_brokers_month_bars(
        provider=provider,
        item=item,
        instrument=instrument,
        bar_size=bar_size,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
        request_sleep_sec=request_sleep_sec,
        print_progress=print_progress,
    )

    if not bars:
        return InteractiveBrokersPipelineResult(
            status="failed",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="No Interactive Brokers bars were downloaded.",
            errors=errors,
        )

    raw_path = write_interactive_brokers_raw_bars_to_parquet(
        bars=bars,
        output_path=paths.local_raw_bars_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=raw_path,
        blob_name=paths.raw_bars_blob_name,
        overwrite=overwrite,
    )

    ohlcv_df = transform_interactive_brokers_bars_to_ohlcv(bars)

    ohlcv_df = filter_ohlcv_to_window(
        ohlcv_df=ohlcv_df,
        item=item,
    )

    if ohlcv_df.empty:
        return InteractiveBrokersPipelineResult(
            status="failed",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="No Interactive Brokers OHLCV data was built.",
            errors=errors,
        )

    validation_result = validate_interactive_brokers_ohlcv(ohlcv_df)

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
        source_url="Interactive Brokers TWS historical data",
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

    return InteractiveBrokersPipelineResult(
        status="uploaded",
        broker=item.broker,
        asset=item.asset,
        broker_symbol=item.broker_symbol,
        year=year,
        month=month,
        message="Raw bars and bronze OHLCV uploaded successfully.",
        errors=errors,
    )


def run_interactive_brokers_plan(
    *,
    plan: list[DownloadPlanItem],
    interval: str,
    instrument_by_asset: dict[str, InteractiveBrokersInstrument],
    blob_service_client: BlobServiceClient,
    container_name: str,
    host: str = DEFAULT_INTERACTIVE_BROKERS_HOST,
    port: int = DEFAULT_INTERACTIVE_BROKERS_PORT,
    client_id: int = DEFAULT_INTERACTIVE_BROKERS_CLIENT_ID,
    readonly: bool = True,
    timeout_sec: int = DEFAULT_INTERACTIVE_BROKERS_TIMEOUT_SEC,
    data_dir: Path = Path("data"),
    bar_size: str = DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    max_attempts: int = DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    overwrite: bool = True,
    print_progress: bool = True,
) -> list[InteractiveBrokersPipelineResult]:
    results: list[InteractiveBrokersPipelineResult] = []

    provider = InteractiveBrokersProvider(
        host=host,
        port=port,
        client_id=client_id,
        readonly=readonly,
        timeout=timeout_sec,
    )

    try:
        for index, item in enumerate(plan):
            instrument = instrument_by_asset.get(item.asset)

            if instrument is None:
                results.append(
                    InteractiveBrokersPipelineResult(
                        status="failed",
                        broker=item.broker,
                        asset=item.asset,
                        broker_symbol=item.broker_symbol,
                        year=item.window.start_date.year,
                        month=item.window.start_date.month,
                        message=(
                            "Missing Interactive Brokers instrument mapping "
                            f"for asset: {item.asset}"
                        ),
                        errors=[],
                    )
                )
                continue

            result = run_interactive_brokers_plan_item(
                item=item,
                interval=interval,
                instrument=instrument,
                provider=provider,
                blob_service_client=blob_service_client,
                container_name=container_name,
                data_dir=data_dir,
                bar_size=bar_size,
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

    finally:
        provider.disconnect()

    return results
