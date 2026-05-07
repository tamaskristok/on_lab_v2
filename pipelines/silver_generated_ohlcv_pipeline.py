from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient

from metadata.silver_manifest import (
    build_silver_generated_ohlcv_manifest,
    write_silver_manifest,
)
from metadata.success_marker import write_success_marker
from paths.silver_paths import build_silver_generated_ohlcv_paths
from uploaders.azure_blob import upload_file


@dataclass(frozen=True)
class SilverGeneratedOhlcvResult:
    status: str
    asset: str
    year: int
    month: int
    message: str
    row_count: int


def _success_exists(
    *,
    blob_service_client: BlobServiceClient,
    container_name: str,
    success_blob_name: str,
) -> bool:
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=success_blob_name,
    )

    return blob_client.exists()


def _select_month_asset_df(
    *,
    silver_ohlcv_df: pd.DataFrame,
    asset: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    return (
        silver_ohlcv_df[
            (silver_ohlcv_df["asset"] == asset)
            & (silver_ohlcv_df["year"] == year)
            & (silver_ohlcv_df["month"] == month)
        ]
        .copy()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def write_and_upload_silver_generated_ohlcv_item(
    *,
    silver_ohlcv_df: pd.DataFrame,
    asset: str,
    interval: str,
    year: int,
    month: int,
    generation_method_id: int,
    generation_method: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    overwrite: bool = False,
) -> SilverGeneratedOhlcvResult:
    paths = build_silver_generated_ohlcv_paths(
        asset=asset,
        interval=interval,
        year=year,
        month=month,
        generation_method_id=generation_method_id,
        data_dir=data_dir,
    )

    if not overwrite and _success_exists(
        blob_service_client=blob_service_client,
        container_name=container_name,
        success_blob_name=paths.success_blob_name,
    ):
        return SilverGeneratedOhlcvResult(
            status="skipped",
            asset=asset,
            year=year,
            month=month,
            message="_SUCCESS already exists in Azure.",
            row_count=0,
        )

    month_asset_df = _select_month_asset_df(
        silver_ohlcv_df=silver_ohlcv_df,
        asset=asset,
        year=year,
        month=month,
    )

    if month_asset_df.empty:
        return SilverGeneratedOhlcvResult(
            status="skipped",
            asset=asset,
            year=year,
            month=month,
            message="No rows for asset/month.",
            row_count=0,
        )

    paths.local_parquet_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    month_asset_df.to_parquet(
        paths.local_parquet_file,
        index=False,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=paths.local_parquet_file,
        blob_name=paths.parquet_blob_name,
        overwrite=overwrite,
    )

    manifest = build_silver_generated_ohlcv_manifest(
        asset=asset,
        interval=interval,
        year=year,
        month=month,
        generation_method_id=generation_method_id,
        generation_method=generation_method,
        parquet_file_path=paths.local_parquet_file,
        silver_ohlcv_df=month_asset_df,
    )

    write_silver_manifest(
        manifest=manifest,
        output_path=paths.local_manifest_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=paths.local_manifest_file,
        blob_name=paths.manifest_blob_name,
        overwrite=overwrite,
    )

    write_success_marker(
        output_path=paths.local_success_file,
    )

    upload_file(
        blob_service_client=blob_service_client,
        container_name=container_name,
        local_path=paths.local_success_file,
        blob_name=paths.success_blob_name,
        overwrite=overwrite,
    )

    return SilverGeneratedOhlcvResult(
        status="uploaded",
        asset=asset,
        year=year,
        month=month,
        message="Silver generated OHLCV uploaded successfully.",
        row_count=len(month_asset_df),
    )


def write_and_upload_silver_generated_ohlcv(
    *,
    silver_ohlcv_df: pd.DataFrame,
    interval: str,
    generation_method_id: int,
    generation_method: str,
    blob_service_client: BlobServiceClient,
    container_name: str,
    data_dir: Path = Path("data"),
    overwrite: bool = False,
    print_progress: bool = True,
) -> list[SilverGeneratedOhlcvResult]:
    results: list[SilverGeneratedOhlcvResult] = []

    month_asset_keys = (
        silver_ohlcv_df[
            [
                "asset",
                "year",
                "month",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "year",
                "month",
                "asset",
            ]
        )
        .to_dict("records")
    )

    for key in month_asset_keys:
        asset = key["asset"]
        year = int(key["year"])
        month = int(key["month"])

        if print_progress:
            print(f"Uploading silver generated OHLCV: {asset} {year}-{month:02d}")

        result = write_and_upload_silver_generated_ohlcv_item(
            silver_ohlcv_df=silver_ohlcv_df,
            asset=asset,
            interval=interval,
            year=year,
            month=month,
            generation_method_id=generation_method_id,
            generation_method=generation_method,
            blob_service_client=blob_service_client,
            container_name=container_name,
            data_dir=data_dir,
            overwrite=overwrite,
        )

        results.append(result)

    return results
