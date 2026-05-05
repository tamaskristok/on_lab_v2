from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from config_loader.csv_config import load_download_period
from paths.calendar_paths import build_silver_calendar_paths
from pipelines.binance_runner import _build_month_date_range
from uploaders.azure_blob import blob_exists, init_azure_client, upload_file


@dataclass(frozen=True)
class CalendarBuildResult:
    status: str
    asset: str
    year: int
    month: int
    message: str
    row_count: int


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _next_month_start(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1)

    return date(year, month + 1, 1)


def _month_keys_between(
    *,
    start_date: date,
    end_date: date,
) -> list[tuple[int, int]]:
    month_keys: list[tuple[int, int]] = []

    current = date(start_date.year, start_date.month, 1)
    last = date(end_date.year, end_date.month, 1)

    while current <= last:
        month_keys.append((current.year, current.month))

        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return month_keys


def _load_date_range_from_config(
    *,
    config_dir: Path,
) -> tuple[date, date]:
    download_period_df = load_download_period(config_dir)
    period = download_period_df.iloc[0]

    start_date = datetime.strptime(
        str(period["start_date"]),
        "%Y-%m-%d",
    ).date()

    end_date = datetime.strptime(
        str(period["end_date"]),
        "%Y-%m-%d",
    ).date()

    return start_date, end_date


def build_24_7_calendar_df(
    *,
    asset: str,
    year: int,
    month: int,
    interval: str = "1m",
) -> pd.DataFrame:
    pandas_freq = "1min" if interval == "1m" else interval

    start = pd.Timestamp(_month_start(year, month), tz="UTC")
    end = pd.Timestamp(_next_month_start(year, month), tz="UTC")

    timestamps = pd.date_range(
        start=start,
        end=end,
        freq=pandas_freq,
        inclusive="left",
    )

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "asset": asset.upper(),
            "year": year,
            "month": month,
            "interval": interval,
            "expected": True,
            "calendar_source": "generated_24_7",
        }
    )


def build_calendar_manifest(
    *,
    asset: str,
    year: int,
    month: int,
    interval: str,
    calendar_source: str,
    calendar_df: pd.DataFrame,
    parquet_file_path: Path,
) -> dict:
    timestamps = pd.to_datetime(
        calendar_df["timestamp"],
        utc=True,
        errors="coerce",
    ).dropna()

    return {
        "metadata": {
            "asset": asset.upper(),
            "interval": interval,
            "year": year,
            "month": month,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "layer": "silver",
            "dataset": "calendar",
        },
        "source": {
            "calendar_source": calendar_source,
        },
        "output": {
            "parquet_file": str(parquet_file_path),
        },
        "calendar": {
            "row_count": len(calendar_df),
            "min_timestamp": (
                timestamps.min().isoformat()
                if not timestamps.empty
                else None
            ),
            "max_timestamp": (
                timestamps.max().isoformat()
                if not timestamps.empty
                else None
            ),
            "expected_count": int(calendar_df["expected"].sum()),
        },
        "columns": list(calendar_df.columns),
    }


def write_json(
    *,
    data: dict,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4,
        )

    return output_path


def write_success_marker(
    *,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("", encoding="utf-8")
    return output_path


def run_24_7_calendar_from_config(
    *,
    asset: str = "BTCUSD",
    start_month: str | None = None,
    end_month: str | None = None,
    interval: str = "1m",
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    data_dir: Path = Path("data"),
    overwrite: bool = True,
) -> list[CalendarBuildResult]:
    start_date, end_date = _build_month_date_range(
        start_month=start_month,
        end_month=end_month,
    )

    if start_date is None or end_date is None:
        start_date, end_date = _load_date_range_from_config(
            config_dir=config_dir,
        )

    month_keys = _month_keys_between(
        start_date=start_date,
        end_date=end_date,
    )

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    results: list[CalendarBuildResult] = []

    for year, month in month_keys:
        paths = build_silver_calendar_paths(
            asset=asset,
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
            results.append(
                CalendarBuildResult(
                    status="skipped",
                    asset=asset.upper(),
                    year=year,
                    month=month,
                    message="_SUCCESS already exists in Azure.",
                    row_count=0,
                )
            )
            continue

        calendar_df = build_24_7_calendar_df(
            asset=asset,
            year=year,
            month=month,
            interval=interval,
        )

        paths.local_calendar_parquet_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        calendar_df.to_parquet(
            paths.local_calendar_parquet_file,
            index=False,
            engine="pyarrow",
        )

        manifest = build_calendar_manifest(
            asset=asset,
            year=year,
            month=month,
            interval=interval,
            calendar_source="generated_24_7",
            calendar_df=calendar_df,
            parquet_file_path=paths.local_calendar_parquet_file,
        )

        manifest_path = write_json(
            data=manifest,
            output_path=paths.local_manifest_file,
        )

        success_path = write_success_marker(
            output_path=paths.local_success_file,
        )

        upload_file(
            blob_service_client=blob_service_client,
            container_name=container_name,
            local_path=paths.local_calendar_parquet_file,
            blob_name=paths.calendar_parquet_blob_name,
            overwrite=overwrite,
        )

        upload_file(
            blob_service_client=blob_service_client,
            container_name=container_name,
            local_path=manifest_path,
            blob_name=paths.manifest_blob_name,
            overwrite=overwrite,
        )

        upload_file(
            blob_service_client=blob_service_client,
            container_name=container_name,
            local_path=success_path,
            blob_name=paths.success_blob_name,
            overwrite=overwrite,
        )

        results.append(
            CalendarBuildResult(
                status="uploaded",
                asset=asset.upper(),
                year=year,
                month=month,
                message="24/7 calendar uploaded successfully.",
                row_count=len(calendar_df),
            )
        )

    return results
