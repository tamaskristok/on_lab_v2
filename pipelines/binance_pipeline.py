
from __future__ import annotations

import time

from dataclasses import dataclass
from pathlib import Path

from azure.storage.blob import BlobServiceClient

from metadata.manifest import build_manifest, write_manifest
from metadata.success_marker import write_success_marker
from paths.data_paths import build_data_paths
from planner.download_plan import DownloadPlanItem
from providers.binance import BinanceProvider, BinanceRequest
from transformers.binance_ohlcv import transform_binance_csv_to_ohlcv
from uploaders.azure_blob import blob_exists, upload_file
from validators.binance_csv import validate_binance_csv
from writers.ohlcv_parquet import write_ohlcv_to_parquet


@dataclass(frozen=True)
class PipelineResult:
    status: str
    broker: str
    asset: str
    broker_symbol: str
    year: int
    month: int
    message: str


def run_binance_plan_item(
    *,
    item: DownloadPlanItem,
    interval: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    overwrite: bool = True,
) -> PipelineResult:
    paths = build_data_paths(
        item=item,
        interval=interval,
        data_dir=data_dir,
    )

    year = item.window.start_date.year
    month = item.window.start_date.month

    if blob_exists(
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_name=paths.success_blob_name,
    ):
        return PipelineResult(
            status="skipped",
            broker=item.broker,
            asset=item.asset,
            broker_symbol=item.broker_symbol,
            year=year,
            month=month,
            message="_SUCCESS already exists in Azure.",
        )

    provider = BinanceProvider()

    request = BinanceRequest(
        symbol=item.broker_symbol,
        interval=interval,
        year=year,
        month=month,
    )

    download_result = provider.download(
        request=request,
        output_dir=paths.local_raw_file.parent,
    )

    csv_path = download_result.files[0]

    validation_result = validate_binance_csv(csv_path)
    ohlcv_df = transform_binance_csv_to_ohlcv(csv_path)

    parquet_path = write_ohlcv_to_parquet(
        df=ohlcv_df,
        output_path=paths.local_bronze_parquet_file,
    )

    manifest = build_manifest(
        provider=item.broker,
        symbol=item.broker_symbol,
        interval=interval,
        year=year,
        month=month,
        source_url=download_result.url,
        raw_file_path=csv_path,
        parquet_file_path=parquet_path,
        validation_result=validation_result,
        ohlcv_df=ohlcv_df,
    )

    write_manifest(
        manifest=manifest,
        output_path=paths.local_manifest_file,
    )

    write_success_marker(
        output_path=paths.local_success_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=csv_path,
        blob_name=paths.raw_blob_name,
        overwrite=overwrite,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=paths.local_bronze_parquet_file,
        blob_name=paths.bronze_parquet_blob_name,
        overwrite=overwrite,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=paths.local_manifest_file,
        blob_name=paths.manifest_blob_name,
        overwrite=overwrite,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=paths.local_success_file,
        blob_name=paths.success_blob_name,
        overwrite=overwrite,
    )

    return PipelineResult(
        status="uploaded",
        broker=item.broker,
        asset=item.asset,
        broker_symbol=item.broker_symbol,
        year=year,
        month=month,
        message="Raw and bronze files uploaded successfully.",
    )

def run_binance_plan(
    *,
    plan: list[DownloadPlanItem],
    interval: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    overwrite: bool = True,
) -> list[PipelineResult]:
    results: list[PipelineResult] = []

    for index, item in enumerate(plan):
        result = run_binance_plan_item(
            item=item,
            interval=interval,
            blob_service_client=blob_service_client,
            container_name=container_name,
            data_dir=data_dir,
            overwrite=overwrite,
        )

        results.append(result)

        is_last_item = index == len(plan) - 1

        if not is_last_item and item.rate_limit_sleep_sec > 0:
            time.sleep(item.rate_limit_sleep_sec)

    return results
