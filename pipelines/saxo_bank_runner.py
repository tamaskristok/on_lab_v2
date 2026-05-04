from __future__ import annotations

from pathlib import Path

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_strategy,
    load_download_period,
    load_saxo_bank_instruments,
)
from pipelines.binance_runner import _build_month_date_range
from pipelines.saxo_bank_pipeline import (
    DEFAULT_SAXO_BANK_BASE_URL,
    DEFAULT_SAXO_BANK_HORIZON,
    DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    DEFAULT_SAXO_BANK_TIMEOUT_SEC,
    SaxoBankPipelineResult,
    build_saxo_bank_instrument_by_asset,
    run_saxo_bank_plan,
)
from planner.download_plan import build_download_plan
from uploaders.azure_blob import init_azure_client


def run_saxo_bank_from_config(
    *,
    access_token: str,
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    interval: str = "1m",
    data_dir: Path = Path("data"),
    start_index: int = 0,
    limit: int | None = None,
    base_url: str = DEFAULT_SAXO_BANK_BASE_URL,
    timeout_sec: int = DEFAULT_SAXO_BANK_TIMEOUT_SEC,
    horizon: int = DEFAULT_SAXO_BANK_HORIZON,
    max_attempts: int = DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    print_progress: bool = True,
    start_month: str | None = None,
    end_month: str | None = None,
) -> list[SaxoBankPipelineResult]:
    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)
    saxo_bank_instruments_df = load_saxo_bank_instruments(config_dir)

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

    saxo_bank_plan = [
        item
        for item in plan
        if item.broker == "saxo_bank"
    ]

    if start_index < 0:
        raise ValueError("start_index cannot be negative")

    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")

    end_index = None if limit is None else start_index + limit
    saxo_bank_plan = saxo_bank_plan[start_index:end_index]

    instrument_by_asset = build_saxo_bank_instrument_by_asset(
        saxo_bank_instruments_df=saxo_bank_instruments_df,
    )

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    return run_saxo_bank_plan(
        plan=saxo_bank_plan,
        interval=interval,
        instrument_by_asset=instrument_by_asset,
        access_token=access_token,
        blob_service_client=blob_service_client,
        container_name=container_name,
        base_url=base_url,
        data_dir=data_dir,
        timeout_sec=timeout_sec,
        horizon=horizon,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
        print_progress=print_progress,
    )
