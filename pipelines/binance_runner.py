from __future__ import annotations

from pathlib import Path

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_strategy,
    load_download_period,
)
from pipelines.binance_pipeline import PipelineResult, run_binance_plan
from planner.download_plan import build_download_plan
from uploaders.azure_blob import init_azure_client


def run_binance_from_config(
    *,
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    interval: str = "1m",
    data_dir: Path = Path("data"),
    start_index: int = 0,
    limit: int | None = None,
) -> list[PipelineResult]:
    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)

    plan = build_download_plan(
        broker_strategy_df=broker_strategy_df,
        broker_asset_matrix_df=broker_asset_matrix_df,
        download_period_df=download_period_df,
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
    )
