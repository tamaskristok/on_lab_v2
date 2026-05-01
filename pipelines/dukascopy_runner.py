from __future__ import annotations

from pathlib import Path

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_asset_settings,
    load_broker_strategy,
    load_download_period,
)
from pipelines.dukascopy_pipeline import DukascopyPipelineResult, run_dukascopy_plan
from planner.download_plan import build_download_plan
from uploaders.azure_blob import init_azure_client


def build_price_scale_by_asset(
    *,
    broker_asset_settings_df,
    broker: str,
) -> dict[str, int]:
    broker_settings = broker_asset_settings_df[
        broker_asset_settings_df["broker"] == broker
    ]

    return {
        row["asset"]: int(row["price_scale"])
        for _, row in broker_settings.iterrows()
    }


def run_dukascopy_from_config(
    *,
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    interval: str = "1m",
    data_dir: Path = Path("data"),
    start_index: int = 0,
    limit: int | None = None,
    timeout_sec: int = 10,
    max_attempts: int = 2,
    retry_sleep_sec: int = 2,
    print_progress: bool = True,
) -> list[DukascopyPipelineResult]:
    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)
    broker_asset_settings_df = load_broker_asset_settings(config_dir)

    plan = build_download_plan(
        broker_strategy_df=broker_strategy_df,
        broker_asset_matrix_df=broker_asset_matrix_df,
        download_period_df=download_period_df,
    )

    dukascopy_plan = [
        item
        for item in plan
        if item.broker == "dukascopy"
    ]

    if start_index < 0:
        raise ValueError("start_index cannot be negative")

    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")

    end_index = None if limit is None else start_index + limit
    dukascopy_plan = dukascopy_plan[start_index:end_index]

    price_scale_by_asset = build_price_scale_by_asset(
        broker_asset_settings_df=broker_asset_settings_df,
        broker="dukascopy",
    )

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    return run_dukascopy_plan(
        plan=dukascopy_plan,
        interval=interval,
        price_scale_by_asset=price_scale_by_asset,
        blob_service_client=blob_service_client,
        container_name=container_name,
        data_dir=data_dir,
        timeout_sec=timeout_sec,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
        print_progress=print_progress,
    )
