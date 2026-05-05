from __future__ import annotations

from pathlib import Path

from config_loader.csv_config import (
    load_broker_asset_matrix,
    load_broker_strategy,
    load_download_period,
    load_interactive_brokers_connection,
    load_interactive_brokers_instruments,
)
from pipelines.binance_runner import _build_month_date_range
from pipelines.interactive_brokers_pipeline import (
    DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    InteractiveBrokersPipelineResult,
    build_interactive_brokers_instrument_by_asset,
    run_interactive_brokers_plan,
)
from planner.download_plan import build_download_plan
from uploaders.azure_blob import init_azure_client


def _parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value

    value_text = str(value).strip().lower()

    if value_text in {"true", "1", "yes", "y"}:
        return True

    if value_text in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"Cannot parse boolean value: {value}")


def load_interactive_brokers_connection_settings(
    *,
    config_dir: Path,
) -> dict:
    connection_df = load_interactive_brokers_connection(config_dir)
    connection = connection_df.iloc[0]

    return {
        "host": str(connection["host"]).strip(),
        "port": int(connection["port"]),
        "client_id": int(connection["client_id"]),
        "readonly": _parse_bool(connection["readonly"]),
        "timeout_sec": int(connection["timeout_sec"]),
    }


def run_interactive_brokers_from_config(
    *,
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    interval: str = "1m",
    data_dir: Path = Path("data"),
    start_index: int = 0,
    limit: int | None = None,
    bar_size: str = DEFAULT_INTERACTIVE_BROKERS_BAR_SIZE,
    max_attempts: int = DEFAULT_INTERACTIVE_BROKERS_MAX_ATTEMPTS,
    retry_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_RETRY_SLEEP_SEC,
    request_sleep_sec: int = DEFAULT_INTERACTIVE_BROKERS_REQUEST_SLEEP_SEC,
    print_progress: bool = True,
    start_month: str | None = None,
    end_month: str | None = None,
) -> list[InteractiveBrokersPipelineResult]:
    broker_strategy_df = load_broker_strategy(config_dir)
    broker_asset_matrix_df = load_broker_asset_matrix(config_dir)
    download_period_df = load_download_period(config_dir)
    interactive_brokers_instruments_df = load_interactive_brokers_instruments(
        config_dir,
    )

    connection_settings = load_interactive_brokers_connection_settings(
        config_dir=config_dir,
    )

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

    interactive_brokers_plan = [
        item
        for item in plan
        if item.broker == "interactive_brokers"
    ]

    if start_index < 0:
        raise ValueError("start_index cannot be negative")

    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")

    end_index = None if limit is None else start_index + limit
    interactive_brokers_plan = interactive_brokers_plan[start_index:end_index]

    instrument_by_asset = build_interactive_brokers_instrument_by_asset(
        interactive_brokers_instruments_df=interactive_brokers_instruments_df,
    )

    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    return run_interactive_brokers_plan(
        plan=interactive_brokers_plan,
        interval=interval,
        instrument_by_asset=instrument_by_asset,
        blob_service_client=blob_service_client,
        container_name=container_name,
        host=connection_settings["host"],
        port=connection_settings["port"],
        client_id=connection_settings["client_id"],
        readonly=connection_settings["readonly"],
        timeout_sec=connection_settings["timeout_sec"],
        data_dir=data_dir,
        bar_size=bar_size,
        max_attempts=max_attempts,
        retry_sleep_sec=retry_sleep_sec,
        request_sleep_sec=request_sleep_sec,
        print_progress=print_progress,
    )
