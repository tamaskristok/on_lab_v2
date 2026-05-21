from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pandas as pd

from uploaders.azure_blob import init_azure_client


def month_range(
    *,
    start_month: str,
    end_month: str,
) -> list[tuple[int, int]]:
    periods = pd.period_range(
        start=start_month,
        end=end_month,
        freq="M",
    )

    return [
        (
            period.year,
            period.month,
        )
        for period in periods
    ]


def silver_parquet_blob_name(
    *,
    method_id: int,
    asset: str,
    year: int,
    month: int,
    interval: str,
) -> str:
    month_text = f"{month:02d}"

    return (
        f"silver/generated_ohlcv/method_{method_id}/"
        f"{asset.lower()}/{year}/{month_text}/"
        f"{asset}-generated-{interval}-{year}-{month_text}.parquet"
    )


def silver_manifest_blob_name(
    *,
    method_id: int,
    asset: str,
    year: int,
    month: int,
) -> str:
    return (
        f"silver/generated_ohlcv/method_{method_id}/"
        f"{asset.lower()}/{year}/{month:02d}/_MANIFEST.json"
    )


def read_parquet_blob(
    *,
    container_client,
    blob_name: str,
) -> pd.DataFrame:
    blob_bytes = container_client.get_blob_client(blob_name).download_blob().readall()

    return pd.read_parquet(
        BytesIO(blob_bytes),
    )


def read_json_blob(
    *,
    container_client,
    blob_name: str,
) -> dict:
    blob_bytes = container_client.get_blob_client(blob_name).download_blob().readall()

    return json.loads(
        blob_bytes.decode("utf-8"),
    )


def load_silver_generated_from_azure(
    *,
    method_id: int,
    asset: str,
    start_month: str,
    end_month: str,
    env_path: Path,
    container_name: str,
    interval: str,
) -> tuple[pd.DataFrame, list[dict]]:
    blob_service_client, resolved_container_name = init_azure_client(
        env_path=env_path,
        container_name=container_name,
    )
    container_client = blob_service_client.get_container_client(
        resolved_container_name,
    )

    frames = []
    manifests = []

    for year, month in month_range(
        start_month=start_month,
        end_month=end_month,
    ):
        parquet_blob_name = silver_parquet_blob_name(
            method_id=method_id,
            asset=asset,
            year=year,
            month=month,
            interval=interval,
        )
        manifest_blob_name = silver_manifest_blob_name(
            method_id=method_id,
            asset=asset,
            year=year,
            month=month,
        )

        parquet_blob_client = container_client.get_blob_client(
            parquet_blob_name,
        )

        if not parquet_blob_client.exists():
            continue

        silver_df = read_parquet_blob(
            container_client=container_client,
            blob_name=parquet_blob_name,
        )

        silver_df["source_blob_name"] = parquet_blob_name

        frames.append(silver_df)

        manifest_blob_client = container_client.get_blob_client(
            manifest_blob_name,
        )

        if manifest_blob_client.exists():
            manifest = read_json_blob(
                container_client=container_client,
                blob_name=manifest_blob_name,
            )
            manifest["blob_name"] = manifest_blob_name
            manifests.append(manifest)

    if not frames:
        return pd.DataFrame(), manifests

    silver_df = pd.concat(
        frames,
        ignore_index=True,
    )

    silver_df["timestamp"] = pd.to_datetime(
        silver_df["timestamp"],
        utc=True,
        errors="coerce",
    )

    silver_df["asset"] = silver_df["asset"].str.upper()

    return (
        silver_df
        .sort_values("timestamp")
        .reset_index(drop=True),
        manifests,
    )


def timestamp_to_chart_time(timestamp: pd.Timestamp) -> int:
    return int(timestamp.timestamp())


def build_method_comparison_df(
    *,
    method_0_df: pd.DataFrame,
    method_1_df: pd.DataFrame,
) -> pd.DataFrame:
    m0 = method_0_df[
        [
            "timestamp",
            "close",
        ]
    ].copy()

    m1 = method_1_df[
        [
            "timestamp",
            "close",
        ]
    ].copy()

    m0 = m0.rename(
        columns={
            "close": "method_0_close",
        }
    )

    m1 = m1.rename(
        columns={
            "close": "method_1_close",
        }
    )

    comparison_df = m0.merge(
        m1,
        on="timestamp",
        how="inner",
    )

    comparison_df["diff"] = (
        comparison_df["method_1_close"]
        - comparison_df["method_0_close"]
    )

    comparison_df["diff_pct"] = (
        comparison_df["diff"]
        / comparison_df["method_0_close"]
        * 100
    )

    comparison_df["abs_diff"] = comparison_df["diff"].abs()
    comparison_df["abs_diff_pct"] = comparison_df["diff_pct"].abs()

    return (
        comparison_df
        .dropna(
            subset=[
                "timestamp",
                "method_0_close",
                "method_1_close",
                "diff_pct",
                "abs_diff_pct",
            ]
        )
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def build_line_data(
    *,
    df: pd.DataFrame,
    value_column: str,
) -> list[dict]:
    chart_df = df.copy()

    chart_df["timestamp"] = pd.to_datetime(
        chart_df["timestamp"],
        utc=True,
        errors="coerce",
    )

    chart_df = (
        chart_df
        .dropna(subset=["timestamp", value_column])
        .sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
    )

    return [
        {
            "time": timestamp_to_chart_time(row.timestamp),
            "value": float(getattr(row, value_column)),
        }
        for row in chart_df.itertuples(index=False)
    ]


def build_diff_pct_data(
    comparison_df: pd.DataFrame,
) -> list[dict]:
    rows = []

    for row in comparison_df.itertuples(index=False):
        diff_pct = float(row.diff_pct)

        rows.append(
            {
                "time": timestamp_to_chart_time(row.timestamp),
                "value": diff_pct,
                "color": "rgba(239, 83, 80, 0.6)"
                if diff_pct < 0
                else "rgba(38, 166, 154, 0.6)",
            }
        )

    return rows


def build_abs_diff_pct_data(
    comparison_df: pd.DataFrame,
) -> list[dict]:
    return [
        {
            "time": timestamp_to_chart_time(row.timestamp),
            "value": float(row.abs_diff_pct),
        }
        for row in comparison_df.itertuples(index=False)
    ]