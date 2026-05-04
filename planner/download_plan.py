from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from planner.time_windows import TimeWindow, generate_daily_windows, generate_monthly_windows


@dataclass(frozen=True)
class DownloadPlanItem:
    broker: str
    asset: str
    broker_symbol: str
    window_type: str
    window: TimeWindow
    rate_limit_sleep_sec: int


def build_download_plan(
    broker_strategy_df: pd.DataFrame,
    broker_asset_matrix_df: pd.DataFrame,
    download_period_df: pd.DataFrame,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[DownloadPlanItem]:
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be provided together")

    if start_date is None or end_date is None:
        period = download_period_df.iloc[0]

        start_date = datetime.strptime(period["start_date"], "%Y-%m-%d").date()
        end_date = datetime.strptime(period["end_date"], "%Y-%m-%d").date()

    plan: list[DownloadPlanItem] = []

    active_brokers = broker_strategy_df[
        broker_strategy_df["enabled"] == True
    ]

    asset_columns = [
        column
        for column in broker_asset_matrix_df.columns
        if column != "broker"
    ]

    for _, strategy in active_brokers.iterrows():
        broker = strategy["broker"]
        window_type = strategy["window_type"]
        rate_limit_sleep_sec = int(strategy["rate_limit_sleep_sec"])

        broker_rows = broker_asset_matrix_df[
            broker_asset_matrix_df["broker"] == broker
        ]

        if broker_rows.empty:
            continue

        broker_row = broker_rows.iloc[0]

        if window_type == "monthly":
            windows = generate_monthly_windows(start_date, end_date)
        elif window_type == "daily":
            windows = generate_daily_windows(start_date, end_date)
        else:
            raise ValueError(f"Unsupported window_type: {window_type}")

        for window in windows:
            for asset in asset_columns:
                broker_symbol = broker_row[asset]

                if pd.isna(broker_symbol):
                    continue

                broker_symbol = str(broker_symbol).strip()

                if broker_symbol == "" or broker_symbol == "NOK":
                    continue

                plan.append(
                    DownloadPlanItem(
                        broker=broker,
                        asset=asset,
                        broker_symbol=broker_symbol,
                        window_type=window_type,
                        window=window,
                        rate_limit_sleep_sec=rate_limit_sleep_sec,
                    )
                )

    return plan
