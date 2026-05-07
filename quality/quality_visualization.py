from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def build_asset_quality_ranking(
    generated_df: pd.DataFrame,
) -> pd.DataFrame:
    asset_quality_df = (
        generated_df
        .groupby("asset", as_index=False)
        .agg(
            total_rows=("time", "count"),
            generated_rows=("is_generated", "sum"),
            avg_broker_count=("broker_count", "mean"),
            max_close_diff_pct=("close_diff_pct", "max"),
        )
    )

    missing_df = (
        generated_df[generated_df["consensus_quality"] == "missing"]
        .groupby("asset", as_index=False)
        .agg(
            missing_rows=("time", "count"),
        )
    )

    single_source_df = (
        generated_df[generated_df["consensus_quality"] == "single_source"]
        .groupby("asset", as_index=False)
        .agg(
            single_source_rows=("time", "count"),
        )
    )

    asset_quality_df = asset_quality_df.merge(
        missing_df,
        on="asset",
        how="left",
    )

    asset_quality_df = asset_quality_df.merge(
        single_source_df,
        on="asset",
        how="left",
    )

    asset_quality_df["missing_rows"] = asset_quality_df[
        "missing_rows"
    ].fillna(0)

    asset_quality_df["single_source_rows"] = asset_quality_df[
        "single_source_rows"
    ].fillna(0)

    asset_quality_df["coverage_ratio"] = (
        asset_quality_df["generated_rows"] / asset_quality_df["total_rows"]
    )

    asset_quality_df["multi_source_rows"] = (
        asset_quality_df["generated_rows"]
        - asset_quality_df["single_source_rows"]
    )

    asset_quality_df["multi_source_ratio"] = (
        asset_quality_df["multi_source_rows"]
        / asset_quality_df["total_rows"]
    )

    return (
        asset_quality_df
        .sort_values(
            [
                "coverage_ratio",
                "multi_source_ratio",
                "avg_broker_count",
                "max_close_diff_pct",
            ],
            ascending=[
                False,
                False,
                False,
                True,
            ],
        )
        .reset_index(drop=True)
    )


def get_best_and_worst_assets(
    asset_quality_ranking_df: pd.DataFrame,
) -> tuple[str, str]:
    if asset_quality_ranking_df.empty:
        raise ValueError("asset_quality_ranking_df is empty")

    best_asset = asset_quality_ranking_df.iloc[0]["asset"]
    worst_asset = asset_quality_ranking_df.iloc[-1]["asset"]

    return best_asset, worst_asset


def get_broker_close_columns(
    generated_df: pd.DataFrame,
) -> list[str]:
    return [
        column
        for column in generated_df.columns
        if column.endswith("_close")
        and not column.startswith("generated_")
        and not column.startswith("close_")
    ]


def build_asset_brokers_and_generated_figure(
    generated_df: pd.DataFrame,
    *,
    asset: str,
) -> go.Figure:
    plot_df = generated_df[
        generated_df["asset"] == asset
    ].copy()

    plot_df = plot_df.sort_values("time")

    close_columns = get_broker_close_columns(plot_df)

    fig = go.Figure()

    for column in close_columns:
        fig.add_trace(
            go.Scatter(
                x=plot_df["time"],
                y=plot_df[column],
                mode="lines",
                name=column,
            )
        )

    fig.add_trace(
        go.Scatter(
            x=plot_df["time"],
            y=plot_df["generated_close"],
            mode="lines",
            name="generated_close",
            line=dict(
                dash="dash",
                width=3,
            ),
        )
    )

    fig.update_layout(
        title=f"{asset} close árfolyam - brokerek + generated",
        xaxis_title="Idő",
        yaxis_title="Close",
        hovermode="x unified",
        height=650,
    )

    return fig


def show_best_and_worst_asset_figures(
    generated_df: pd.DataFrame,
) -> tuple[pd.DataFrame, str, str]:
    asset_quality_ranking_df = build_asset_quality_ranking(
        generated_df,
    )

    best_asset, worst_asset = get_best_and_worst_assets(
        asset_quality_ranking_df,
    )

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

    return asset_quality_ranking_df, best_asset, worst_asset
