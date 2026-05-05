from __future__ import annotations

from pathlib import Path

import pandas as pd

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_asset_settings,
    load_broker_strategy,
    load_download_period,
    load_interactive_brokers_instruments,
    load_saxo_bank_instruments,
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
from pipelines.saxo_bank_pipeline import (
    DEFAULT_SAXO_BANK_BASE_URL,
    DEFAULT_SAXO_BANK_HORIZON,
    DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    DEFAULT_SAXO_BANK_TIMEOUT_SEC,
    build_saxo_bank_instrument_by_asset,
    run_saxo_bank_plan,
)
from pipelines.interactive_brokers_pipeline import (
    DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    DEFAULT_INTERACTIVE_BROKERS_CLIENT_ID,
    DEFAULT_INTERACTIVE_BROKERS_HOST,
    DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    DEFAULT_INTERACTIVE_BROKERS_PORT,
    DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    DEFAULT_INTERACTIVE_BROKERS_TIMEOUT_SEC,
    build_interactive_brokers_instrument_by_asset,
    run_interactive_brokers_plan,
)

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


def _requires_saxo_bank(
    brokers: list[str] | None,
) -> bool:
    return brokers is None or "saxo_bank" in brokers


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
    saxo_access_token: str | None = None,
    saxo_base_url: str = DEFAULT_SAXO_BANK_BASE_URL,
    saxo_timeout_sec: int = DEFAULT_SAXO_BANK_TIMEOUT_SEC,
    saxo_horizon: int = DEFAULT_SAXO_BANK_HORIZON,
    saxo_max_attempts: int = DEFAULT_SAXO_BANK_MAX_ATTEMPTS,
    saxo_retry_sleep_sec: int = DEFAULT_SAXO_BANK_RETRY_SLEEP_SEC,
    saxo_print_progress: bool = True,
    interactive_brokers_host: str = DEFAULT_INTERACTIVE_BROKERS_HOST,
    interactive_brokers_port: int = DEFAULT_INTERACTIVE_BROKERS_PORT,
    interactive_brokers_client_id: int = DEFAULT_INTERACTIVE_BROKERS_CLIENT_ID,
    interactive_brokers_readonly: bool = True,
    interactive_brokers_timeout_sec: int = DEFAULT_INTERACTIVE_BROKERS_TIMEOUT_SEC,
    interactive_brokers_bar_size: str = DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    interactive_brokers_max_attempts: int = DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    interactive_brokers_retry_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    interactive_brokers_request_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    interactive_brokers_print_progress: bool = True,

) -> pd.DataFrame:
    if _requires_saxo_bank(brokers) and not saxo_access_token:
        raise ValueError("saxo_access_token is required when running saxo_bank")

    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)
    broker_asset_settings_df = load_broker_asset_settings(config_dir)
    saxo_bank_instruments_df = load_saxo_bank_instruments(config_dir)
    interactive_brokers_instruments_df = load_interactive_brokers_instruments(config_dir)

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

    saxo_instrument_by_asset = build_saxo_bank_instrument_by_asset(
        saxo_bank_instruments_df=saxo_bank_instruments_df,
    )

    interactive_brokers_instrument_by_asset = build_interactive_brokers_instrument_by_asset(
        interactive_brokers_instruments_df=interactive_brokers_instruments_df,
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

        saxo_bank_plan = _filter_plan_by_broker(
            plan=month_plan,
            broker="saxo_bank",
        )

        interactive_brokers_plan = _filter_plan_by_broker(
            plan=month_plan,
            broker="interactive_brokers",
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
            print("Binance: nincs futtatando item.")

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
            print("Dukascopy: nincs futtatando item.")

        if saxo_bank_plan:
            print("Saxo Bank indul...")

            saxo_bank_results = run_saxo_bank_plan(
                plan=saxo_bank_plan,
                interval=interval,
                instrument_by_asset=saxo_instrument_by_asset,
                access_token=saxo_access_token,
                blob_service_client=blob_service_client,
                container_name=container_name,
                base_url=saxo_base_url,
                data_dir=data_dir,
                timeout_sec=saxo_timeout_sec,
                horizon=saxo_horizon,
                max_attempts=saxo_max_attempts,
                retry_sleep_sec=saxo_retry_sleep_sec,
                print_progress=saxo_print_progress,
            )

            all_results.extend(saxo_bank_results)
        else:
            print("Saxo Bank: nincs futtatando item.")

        if interactive_brokers_plan:
            print("Interactive Brokers indul...")

            interactive_brokers_results = run_interactive_brokers_plan(
                plan=interactive_brokers_plan,
                interval=interval,
                instrument_by_asset=interactive_brokers_instrument_by_asset,
                blob_service_client=blob_service_client,
                container_name=container_name,
                host=interactive_brokers_host,
                port=interactive_brokers_port,
                client_id=interactive_brokers_client_id,
                readonly=interactive_brokers_readonly,
                timeout_sec=interactive_brokers_timeout_sec,
                data_dir=data_dir,
                bar_size=interactive_brokers_bar_size,
                max_attempts=interactive_brokers_max_attempts,
                retry_sleep_sec=interactive_brokers_retry_sleep_sec,
                request_sleep_sec=interactive_brokers_request_sleep_sec,
                print_progress=interactive_brokers_print_progress,
            )

            all_results.extend(interactive_brokers_results)
        else:
            print("Interactive Brokers: nincs futtatando item.")


    return _results_to_dataframe(all_results)
