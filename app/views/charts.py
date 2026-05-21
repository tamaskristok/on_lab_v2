from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from app.components.lightweight_chart import render_method_comparison_chart
from app.services.silver_chart_data import (
    build_abs_diff_pct_data,
    build_diff_pct_data,
    build_line_data,
    build_method_comparison_df,
    load_silver_generated_from_azure,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
ASSET_DIR = Path(__file__).parents[1] / "assets"

BROKER_ASSET_MATRIX_PATH = CONFIG_DIR / "broker_asset_matrix.csv"
LIGHTWEIGHT_CHARTS_JS_PATH = ASSET_DIR / "lightweight-charts.standalone.production.js"

ENV_PATH = PROJECT_ROOT / ".env"
CONTAINER_NAME = "market-data"
INTERVAL = "1m"


@st.cache_data
def load_broker_asset_matrix() -> pd.DataFrame:
    return pd.read_csv(BROKER_ASSET_MATRIX_PATH)


@st.cache_data
def load_lightweight_charts_js() -> str:
    return LIGHTWEIGHT_CHARTS_JS_PATH.read_text(
        encoding="utf-8",
    )


def get_assets(
    broker_asset_matrix_df: pd.DataFrame,
) -> list[str]:
    asset_columns = [
        column
        for column in broker_asset_matrix_df.columns
        if column != "broker"
    ]

    return sorted(asset_columns)


def build_selected_broker_share_df(
    method_1_df: pd.DataFrame,
) -> pd.DataFrame:
    if "selected_broker" not in method_1_df.columns:
        return pd.DataFrame(
            columns=[
                "selected_broker",
                "rows",
                "share_pct",
            ]
        )

    selected_broker_series = method_1_df["selected_broker"].dropna()

    if selected_broker_series.empty:
        return pd.DataFrame(
            columns=[
                "selected_broker",
                "rows",
                "share_pct",
            ]
        )

    counts_df = (
        selected_broker_series
        .value_counts()
        .rename_axis("selected_broker")
        .reset_index(name="rows")
    )

    counts_df["share_pct"] = (
        counts_df["rows"]
        / counts_df["rows"].sum()
        * 100
    )

    return counts_df


def show_method_comparison_metrics(
    comparison_df: pd.DataFrame,
) -> None:
    correlation = comparison_df["method_0_close"].corr(
        comparison_df["method_1_close"]
    )

    max_abs_diff_row = comparison_df.loc[
        comparison_df["abs_diff_pct"].idxmax()
    ]

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)

    metric_col_1.metric(
        "Korreláció",
        f"{correlation:.6f}",
    )

    metric_col_2.metric(
        "Átlagos abs eltérés",
        f"{comparison_df['abs_diff_pct'].mean():.4f}%",
    )

    metric_col_3.metric(
        "Max abs eltérés",
        f"{comparison_df['abs_diff_pct'].max():.4f}%",
    )

    metric_col_4.metric(
        "95% percentilis",
        f"{comparison_df['abs_diff_pct'].quantile(0.95):.4f}%",
    )

    metric_col_5, metric_col_6, metric_col_7 = st.columns(3)

    metric_col_5.metric(
        "Átlagos előjeles eltérés",
        f"{comparison_df['diff_pct'].mean():.4f}%",
    )

    metric_col_6.metric(
        "> 0.1% eltérés aránya",
        f"{(comparison_df['abs_diff_pct'] > 0.1).mean() * 100:.2f}%",
    )

    metric_col_7.metric(
        "Sorok",
        f"{len(comparison_df):,}",
    )

    st.write(
        {
            "start": comparison_df["timestamp"].min(),
            "end": comparison_df["timestamp"].max(),
            "max_abs_diff_timestamp": max_abs_diff_row["timestamp"],
            "max_abs_diff_method_0_close": max_abs_diff_row["method_0_close"],
            "max_abs_diff_method_1_close": max_abs_diff_row["method_1_close"],
            "max_abs_diff_pct_signed": max_abs_diff_row["diff_pct"],
            "max_abs_diff_pct": max_abs_diff_row["abs_diff_pct"],
        }
    )


st.title("Charts")

if not LIGHTWEIGHT_CHARTS_JS_PATH.exists():
    st.error(
        "Missing Lightweight Charts JavaScript file: "
        "app/assets/lightweight-charts.standalone.production.js"
    )
    st.stop()

broker_asset_matrix_df = load_broker_asset_matrix()

asset_options = get_assets(
    broker_asset_matrix_df,
)

asset = st.selectbox(
    "Ticker",
    options=asset_options,
)

date_col_1, date_col_2 = st.columns(2)

with date_col_1:
    start_month = st.text_input(
        "Kezdő hónap",
        value="2024-01",
        help="Formátum: YYYY-MM",
    )

with date_col_2:
    end_month = st.text_input(
        "Záró hónap",
        value="2024-02",
        help="Formátum: YYYY-MM",
    )

comparison_requested = st.button(
    "Method összehasonlítás kirajzolása",
    use_container_width=True,
)

if comparison_requested:
    with st.spinner("Method 0 és method 1 silver adatok betöltése..."):
        method_0_df, method_0_manifests = load_silver_generated_from_azure(
            method_id=0,
            asset=asset,
            start_month=start_month,
            end_month=end_month,
            env_path=ENV_PATH,
            container_name=CONTAINER_NAME,
            interval=INTERVAL,
        )

        method_1_df, method_1_manifests = load_silver_generated_from_azure(
            method_id=1,
            asset=asset,
            start_month=start_month,
            end_month=end_month,
            env_path=ENV_PATH,
            container_name=CONTAINER_NAME,
            interval=INTERVAL,
        )

    if method_0_df.empty:
        st.warning("Nincs method 0 silver adat a kiválasztott paraméterekre.")
        st.stop()

    if method_1_df.empty:
        st.warning("Nincs method 1 silver adat a kiválasztott paraméterekre.")
        st.stop()

    comparison_df = build_method_comparison_df(
        method_0_df=method_0_df,
        method_1_df=method_1_df,
    )

    if comparison_df.empty:
        st.warning("Nincs közös timestamp method 0 és method 1 között.")
        st.stop()

    st.subheader(f"{asset} / method 0 vs method 1 / {start_month} - {end_month}")

    show_method_comparison_metrics(
        comparison_df,
    )

    method_0_data = build_line_data(
        df=comparison_df,
        value_column="method_0_close",
    )

    method_1_data = build_line_data(
        df=comparison_df,
        value_column="method_1_close",
    )

    diff_pct_data = build_diff_pct_data(
        comparison_df,
    )

    abs_diff_pct_data = build_abs_diff_pct_data(
        comparison_df,
    )

    render_method_comparison_chart(
        lightweight_charts_js=load_lightweight_charts_js(),
        method_0_data=method_0_data,
        method_1_data=method_1_data,
        diff_pct_data=diff_pct_data,
        abs_diff_pct_data=abs_diff_pct_data,
    )

    selected_broker_share_df = build_selected_broker_share_df(
        method_1_df,
    )

    if not selected_broker_share_df.empty:
        st.subheader("Method 1 selected broker megoszlás")
        st.dataframe(
            selected_broker_share_df,
            use_container_width=True,
        )

    st.subheader("Manifestek")

    with st.expander("Method 0 manifestek"):
        if method_0_manifests:
            for manifest in method_0_manifests:
                st.json(manifest)
        else:
            st.info("Nem található method 0 manifest.")

    with st.expander("Method 1 manifestek"):
        if method_1_manifests:
            for manifest in method_1_manifests:
                st.json(manifest)
        else:
            st.info("Nem található method 1 manifest.")