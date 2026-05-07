from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SilverGeneratedOhlcvPaths:
    local_parquet_file: Path
    local_manifest_file: Path
    local_success_file: Path

    parquet_blob_name: str
    manifest_blob_name: str
    success_blob_name: str


def build_silver_generated_ohlcv_paths(
    *,
    asset: str,
    interval: str,
    year: int,
    month: int,
    generation_method_id: int,
    data_dir: Path = Path("data"),
) -> SilverGeneratedOhlcvPaths:
    month_text = f"{month:02d}"
    asset_folder = asset.lower()

    file_name = f"{asset}-generated-{interval}-{year}-{month_text}.parquet"

    method_folder = f"method_{generation_method_id}"

    local_dir = (
        data_dir
        / "silver"
        / "generated_ohlcv"
        / method_folder
        / asset_folder
        / str(year)
        / month_text
    )

    local_parquet_file = local_dir / file_name
    local_manifest_file = local_dir / "_MANIFEST.json"
    local_success_file = local_dir / "_SUCCESS"

    parquet_blob_name = (
        f"silver/generated_ohlcv/{method_folder}/"
        f"{asset_folder}/{year}/{month_text}/{file_name}"
    )
    manifest_blob_name = (
        f"silver/generated_ohlcv/{method_folder}/"
        f"{asset_folder}/{year}/{month_text}/_MANIFEST.json"
    )
    success_blob_name = (
        f"silver/generated_ohlcv/{method_folder}/"
        f"{asset_folder}/{year}/{month_text}/_SUCCESS"
    )

    return SilverGeneratedOhlcvPaths(
        local_parquet_file=local_parquet_file,
        local_manifest_file=local_manifest_file,
        local_success_file=local_success_file,
        parquet_blob_name=parquet_blob_name,
        manifest_blob_name=manifest_blob_name,
        success_blob_name=success_blob_name,
    )
