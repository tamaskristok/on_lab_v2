from __future__ import annotations

from pathlib import Path

import pandas as pd

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_asset_settings,
    load_broker_strategy,
    load_download_period,
)
from pipelines.binance_pipeline import run_binance_plan
from pipelines.binance_runner import _build_month_date_range
from pipelines.dukascopy_pipeline import (
    DEFAULT_DUKASCOPY_MAX_ATTEMPTS,
    DEFAULT_DUKASCOPY_RETRY_SLEEP_SEC,
    DEFAULT_DUKASCOPY_TIMEOUT_SEC,
    run_dukascopy_plan,
)
from pipelines.dukascopy_runner import build_price_scale_by_asset
from planner.download_plan import DownloadPlanItem, build_download_plan
from uploaders.azure_blob import init_azure_client


def _filter_plan_by_broker(
    plan: list[DownloadPlanItem],
    broker: str,
) -> list[DownloadPlanItem]:
    return [
        item
        for item in plan
        if item.broker == broker
    ]


def _filter_plan_by_brokers(
    plan: list[DownloadPlanItem],
    brokers: list[str] | None,
) -> list[DownloadPlanItem]:
    if brokers is None:
        return plan

    allowed_brokers = set(brokers)

    return [
        item
        for item in plan
        if item.broker in allowed_brokers
    ]


def _filter_plan_by_assets(
    plan: list[DownloadPlanItem],
    assets: list[str] | None,
) -> list[DownloadPlanItem]:
    if assets is None:
        return plan

    allowed_assets = set(assets)

    return [
        item
        for item in plan
        if item.asset in allowed_assets
    ]


def _month_keys_from_plan(
    plan: list[DownloadPlanItem],
) -> list[tuple[int, int]]:
    return sorted(
        {
            (item.window.start_date.year, item.window.start_date.month)
            for item in plan
        }
    )


def _filter_plan_by_month(
    plan: list[DownloadPlanItem],
    year: int,
    month: int,
) -> list[DownloadPlanItem]:
    return [
        item
        for item in plan
        if item.window.start_date.year == year
        and item.window.start_date.month == month
    ]


def _results_to_dataframe(results: list) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()

    return pd.DataFrame(
        [result.__dict__ for result in results]
    )


def run_monthly_ingestion_from_config(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | None = None,
    assets: list[str] | None = None,
    interval: str = "1m",
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    data_dir: Path = Path("data"),
    binance_max_attempts: int = 2,
    binance_retry_sleep_sec: int = 1,
    dukascopy_timeout_sec: int = DEFAULT_DUKASCOPY_TIMEOUT_SEC,
    dukascopy_max_attempts: int = DEFAULT_DUKASCOPY_MAX_ATTEMPTS,
    dukascopy_retry_sleep_sec: int = DEFAULT_DUKASCOPY_RETRY_SLEEP_SEC,
    dukascopy_print_progress: bool = True,
) -> pd.DataFrame:
    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)
    broker_asset_settings_df = load_broker_asset_settings(config_dir)

    start_date, end_date = _build_month_date_range(
        start_month=start_month,
        end_month=end_month,
    )

    if start_date is None or end_date is None:
        base_plan = build_download_plan(
            broker_strategy_df=broker_strategy_df,
            broker_asset_matrix_df=broker_asset_matrix_df,
            download_period_df=download_period_df,
        )
    else:
        base_plan = build_download_plan(
            broker_strategy_df=broker_strategy_df,
            broker_asset_matrix_df=broker_asset_matrix_df,
            download_period_df=download_period_df,
            start_date=start_date,
            end_date=end_date,
        )

    base_plan = _filter_plan_by_brokers(
        plan=base_plan,
        brokers=brokers,
    )

    base_plan = _filter_plan_by_assets(
        plan=base_plan,
        assets=assets,
    )

    month_keys = _month_keys_from_plan(base_plan)

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    price_scale_by_asset = build_price_scale_by_asset(
        broker_asset_settings_df=broker_asset_settings_df,
        broker="dukascopy",
    )

    all_results = []

    for year, month in month_keys:
        print(f"\n=== {year}-{month:02d} ===")

        month_plan = _filter_plan_by_month(
            plan=base_plan,
            year=year,
            month=month,
        )

        binance_plan = _filter_plan_by_broker(
            plan=month_plan,
            broker="binance",
        )

        dukascopy_plan = _filter_plan_by_broker(
            plan=month_plan,
            broker="dukascopy",
        )

        if binance_plan:
            print("Binance indul...")

            binance_results = run_binance_plan(
                plan=binance_plan,
                interval=interval,
                blob_service_client=blob_service_client,
                container_name=container_name,
                data_dir=data_dir,
                max_attempts=binance_max_attempts,
                retry_sleep_sec=binance_retry_sleep_sec,
            )

            all_results.extend(binance_results)
        else:
            print("Binance: nincs futtatandó item.")

        if dukascopy_plan:
            print("Dukascopy indul...")

            dukascopy_results = run_dukascopy_plan(
                plan=dukascopy_plan,
                interval=interval,
                price_scale_by_asset=price_scale_by_asset,
                blob_service_client=blob_service_client,
                container_name=container_name,
                data_dir=data_dir,
                timeout_sec=dukascopy_timeout_sec,
                max_attempts=dukascopy_max_attempts,
                retry_sleep_sec=dukascopy_retry_sleep_sec,
                print_progress=dukascopy_print_progress,
            )

            all_results.extend(dukascopy_results)
        else:
            print("Dukascopy: nincs futtatandó item.")

    return _results_to_dataframe(all_results)
