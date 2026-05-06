from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from config_loader.csv_config import (
    load_download_period,
    load_interactive_brokers_calendar_instruments,
)
from paths.calendar_paths import build_silver_calendar_paths
from pipelines.binance_runner import _build_month_date_range
from pipelines.calendar_24_7_runner import (
    _month_keys_between,
    build_calendar_manifest,
    write_json,
    write_success_marker,
)
from pipelines.interactive_brokers_runner import (
    load_interactive_brokers_connection_settings,
)
from providers.interactive_brokers import InteractiveBrokersProvider
from providers.interactive_brokers_schedule import (
    InteractiveBrokersScheduleRequest,
    download_interactive_brokers_schedule,
)
from uploaders.azure_blob import blob_exists, init_azure_client, upload_file


@dataclass(frozen=True)
class InteractiveBrokersCalendarBuildResult:
    status: str
    asset: str
    year: int
    month: int
    message: str
    row_count: int


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


def _next_month_start(
    *,
    year: int,
    month: int,
) -> date:
    if month == 12:
        return date(year + 1, 1, 1)

    return date(year, month + 1, 1)


def _parse_ib_schedule_datetime(
    *,
    value: str,
    calendar_timezone: str,
) -> pd.Timestamp:
    local_datetime = datetime.strptime(
        value,
        "%Y%m%d-%H:%M:%S",
    ).replace(
        tzinfo=ZoneInfo(calendar_timezone),
    )

    return pd.Timestamp(local_datetime).tz_convert("UTC")


def build_calendar_df_from_interactive_brokers_sessions(
    *,
    asset: str,
    year: int,
    month: int,
    interval: str,
    calendar_timezone: str,
    sessions,
) -> pd.DataFrame:
    pandas_freq = "1min" if interval == "1m" else interval
    month_start = pd.Timestamp(date(year, month, 1), tz="UTC")
    month_end = pd.Timestamp(
        _next_month_start(year=year, month=month),
        tz="UTC",
    )

    frames: list[pd.DataFrame] = []

    for session in sessions:
        start = _parse_ib_schedule_datetime(
            value=session.start_datetime,
            calendar_timezone=calendar_timezone,
        )
        end = _parse_ib_schedule_datetime(
            value=session.end_datetime,
            calendar_timezone=calendar_timezone,
        )

        start = max(start, month_start)
        end = min(end, month_end)

        if start >= end:
            continue

        timestamps = pd.date_range(
            start=start,
            end=end,
            freq=pandas_freq,
            inclusive="left",
        )

        if timestamps.empty:
            continue

        frames.append(
            pd.DataFrame(
                {
                    "timestamp": timestamps,
                    "asset": asset.upper(),
                    "year": year,
                    "month": month,
                    "interval": interval,
                    "expected": True,
                    "calendar_source": (
                        "interactive_brokers_historical_schedule"
                    ),
                    "calendar_timezone": calendar_timezone,
                    "session_ref_date": session.ref_date,
                }
            )
        )

    if not frames:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "asset",
                "year",
                "month",
                "interval",
                "expected",
                "calendar_source",
                "calendar_timezone",
                "session_ref_date",
            ]
        )

    calendar_df = pd.concat(
        frames,
        ignore_index=True,
    )

    calendar_df = (
        calendar_df.sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"])
        .reset_index(drop=True)
    )

    return calendar_df


def run_interactive_brokers_calendar_from_config(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    assets: list[str] | None = None,
    interval: str = "1m",
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    data_dir: Path = Path("data"),
    num_days: int = 45,
    use_regular_trading_hours: bool = False,
    request_sleep_sec: int = 1,
    overwrite: bool = True,
    print_progress: bool = True,
) -> list[InteractiveBrokersCalendarBuildResult]:
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

    calendar_instruments_df = load_interactive_brokers_calendar_instruments(
        config_dir,
    )

    if assets is not None:
        wanted_assets = {asset.upper() for asset in assets}
        calendar_instruments_df = calendar_instruments_df[
            calendar_instruments_df["asset"].str.upper().isin(wanted_assets)
        ]

    connection_settings = load_interactive_brokers_connection_settings(
        config_dir=config_dir,
    )

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    provider = InteractiveBrokersProvider(
        host=connection_settings["host"],
        port=connection_settings["port"],
        client_id=connection_settings["client_id"],
        readonly=connection_settings["readonly"],
        timeout=connection_settings["timeout_sec"],
    )

    results: list[InteractiveBrokersCalendarBuildResult] = []

    try:
        for year, month in month_keys:
            for _, instrument in calendar_instruments_df.iterrows():
                asset = str(instrument["asset"]).upper()
                calendar_timezone = str(instrument["calendar_timezone"])

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
                        InteractiveBrokersCalendarBuildResult(
                            status="skipped",
                            asset=asset,
                            year=year,
                            month=month,
                            message="_SUCCESS already exists in Azure.",
                            row_count=0,
                        )
                    )
                    continue

                if print_progress:
                    print(f"{asset} {year}-{month:02d} calendar downloading...")

                request = InteractiveBrokersScheduleRequest(
                    con_id=int(instrument["con_id"]),
                    symbol=str(instrument["symbol"]),
                    security_type=str(instrument["security_type"]),
                    exchange=str(instrument["exchange"]),
                    currency=str(instrument["currency"]),
                    end_datetime=datetime.combine(
                        _next_month_start(year=year, month=month),
                        datetime_time.min,
                        tzinfo=timezone.utc,
                    ),
                    num_days=num_days,
                    use_regular_trading_hours=use_regular_trading_hours,
                )

                schedule_result = download_interactive_brokers_schedule(
                    provider=provider,
                    request=request,
                )

                calendar_df = (
                    build_calendar_df_from_interactive_brokers_sessions(
                        asset=asset,
                        year=year,
                        month=month,
                        interval=interval,
                        calendar_timezone=calendar_timezone,
                        sessions=schedule_result.sessions,
                    )
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
                    calendar_source=(
                        "interactive_brokers_historical_schedule"
                    ),
                    calendar_df=calendar_df,
                    parquet_file_path=paths.local_calendar_parquet_file,
                )

                manifest["source"]["calendar_timezone"] = calendar_timezone
                manifest["source"]["num_days"] = num_days
                manifest["source"]["use_regular_trading_hours"] = (
                    use_regular_trading_hours
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
                    InteractiveBrokersCalendarBuildResult(
                        status="uploaded",
                        asset=asset,
                        year=year,
                        month=month,
                        message="IB calendar uploaded successfully.",
                        row_count=len(calendar_df),
                    )
                )

                if request_sleep_sec > 0:
                    time.sleep(request_sleep_sec)

    finally:
        provider.disconnect()

    return results
