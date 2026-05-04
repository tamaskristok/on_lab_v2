from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_strategy,
    load_download_period,
)
from pipelines.binance_pipeline import PipelineResult, run_binance_plan
from planner.download_plan import build_download_plan
from uploaders.azure_blob import init_azure_client


def _parse_month(month_text: str) -> date:
    if len(month_text) != 7 or month_text[4] != "-":
        raise ValueError("Month must use YYYY-MM format")

    year_text = month_text[:4]
    month_number_text = month_text[5:7]

    if not year_text.isdigit() or not month_number_text.isdigit():
        raise ValueError("Month must use YYYY-MM format")

    year = int(year_text)
    month = int(month_number_text)

    if month < 1 or month > 12:
        raise ValueError("Month must be between 01 and 12")

    return date(year, month, 1)


def _month_end(month_start: date) -> date:
    if month_start.month == 12:
        next_month_start = date(month_start.year + 1, 1, 1)
    else:
        next_month_start = date(month_start.year, month_start.month + 1, 1)

    return next_month_start - timedelta(days=1)


def _build_month_date_range(
    start_month: str | None,
    end_month: str | None,
) -> tuple[date | None, date | None]:
    if start_month is None and end_month is None:
        return None, None

    if start_month is None or end_month is None:
        raise ValueError("start_month and end_month must be provided together")

    start_date = _parse_month(start_month)
    end_month_start = _parse_month(end_month)
    end_date = _month_end(end_month_start)

    if start_date > end_date:
        raise ValueError("start_month cannot be after end_month")

    current_month_start = date.today().replace(day=1)

    if end_month_start >= current_month_start:
        raise ValueError("Only fully closed past months can be downloaded")

    return start_date, end_date


def run_binance_from_config(
    *,
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    interval: str = "1m",
    data_dir: Path = Path("data"),
    start_index: int = 0,
    limit: int | None = None,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
    start_month: str | None = None,
    end_month: str | None = None,
) -> list[PipelineResult]:
    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)

    start_date, end_date = _build_month_date_range(
        start_month=start_month,
        end_month=end_month,
    )

    plan = build_download_plan(
        broker_strategy_df=broker_strategy_df,
        broker_asset_matrix_df=broker_asset_matrix_df,
        download_period_df=download_period_df,
        start_date=start_date,
        end_date=end_date,
    )

    binance_plan = [
        item
        for item in plan
        if item.broker == "binance"
    ]

    if start_index < 0:
        raise ValueError("start_index cannot be negative")

    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")

    end_index = None if limit is None else start_index + limit

    binance_plan = binance_plan[start_index:end_index]

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    return run_binance_plan(
        plan=binance_plan,
        interval=interval,
        blob_service_client=blob_service_client,
        container_name=container_name,
        data_dir=data_dir,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
    )
