from __future__ import annotations

from pathlib import Path

import pandas as pd

from quality.broker_ranking import build_broker_ranking
from quality.candle_generation import build_generated_candles
from quality.quality_visualization import (
    build_asset_brokers_and_generated_figure,
    build_asset_quality_ranking,
    get_best_and_worst_assets,
)
from quality.silver_ohlcv import (
    build_silver_ohlcv_from_generated,
    build_silver_ohlcv_summary,
)


def run_silver_quality_from_config(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | str | None = None,
    assets: list[str] | str | None = None,
    interval: str = "1m",
    generation_method: int = 0,
    source: str = "azure",
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    data_dir: Path = Path("data"),
    print_progress: bool = False,
    show_figures: bool = True,
) -> dict[str, pd.DataFrame | str]:
    generated_df = build_generated_candles(
        start_month=start_month,
        end_month=end_month,
        brokers=brokers,
        assets=assets,
        interval=interval,
        method=generation_method,
        source=source,
        config_dir=config_dir,
        env_path=env_path,
        container_name=container_name,
        data_dir=data_dir,
        print_progress=print_progress,
    )

    silver_ohlcv_df = build_silver_ohlcv_from_generated(
        generated_df,
        config_dir=config_dir,
    )

    silver_summary_df = build_silver_ohlcv_summary(
        silver_ohlcv_df,
    )

    broker_ranking_df = build_broker_ranking(
        generated_df,
    )

    asset_quality_ranking_df = build_asset_quality_ranking(
        generated_df,
    )

    best_asset, worst_asset = get_best_and_worst_assets(
        asset_quality_ranking_df,
    )

    if show_figures:
        best_fig = build_asset_brokers_and_generated_figure(
            generated_df,
            asset=best_asset,
        )
        worst_fig = build_asset_brokers_and_generated_figure(
            generated_df,
            asset=worst_asset,
        )

        best_fig.show()
        worst_fig.show()

    return {
        "generated": generated_df,
        "silver_ohlcv": silver_ohlcv_df,
        "silver_summary": silver_summary_df,
        "broker_ranking": broker_ranking_df,
        "asset_quality_ranking": asset_quality_ranking_df,
        "best_asset": best_asset,
        "worst_asset": worst_asset,
    }
