from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SilverCalendarPaths:
    local_calendar_parquet_file: Path
    local_manifest_file: Path
    local_success_file: Path

    calendar_parquet_blob_name: str
    manifest_blob_name: str
    success_blob_name: str


def build_silver_calendar_paths(
    *,
    asset: str,
    interval: str,
    year: int,
    month: int,
    data_dir: Path = Path("data"),
) -> SilverCalendarPaths:
    month_text = f"{month:02d}"
    asset_folder = asset.lower()
    asset_symbol = asset.upper()

    file_name = f"{asset_symbol}-calendar-{interval}-{year}-{month_text}.parquet"

    local_calendar_dir = (
        data_dir
        / "silver"
        / "calendar"
        / asset_folder
        / str(year)
        / month_text
    )

    local_calendar_parquet_file = local_calendar_dir / file_name
    local_manifest_file = local_calendar_dir / "_MANIFEST.json"
    local_success_file = local_calendar_dir / "_SUCCESS"

    calendar_parquet_blob_name = (
        f"silver/calendar/{asset_folder}/{year}/{month_text}/{file_name}"
    )
    manifest_blob_name = (
        f"silver/calendar/{asset_folder}/{year}/{month_text}/_MANIFEST.json"
    )
    success_blob_name = (
        f"silver/calendar/{asset_folder}/{year}/{month_text}/_SUCCESS"
    )

    return SilverCalendarPaths(
        local_calendar_parquet_file=local_calendar_parquet_file,
        local_manifest_file=local_manifest_file,
        local_success_file=local_success_file,
        calendar_parquet_blob_name=calendar_parquet_blob_name,
        manifest_blob_name=manifest_blob_name,
        success_blob_name=success_blob_name,
    )
