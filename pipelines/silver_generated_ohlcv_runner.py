from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.silver_generated_ohlcv_pipeline import (
    write_and_upload_silver_generated_ohlcv,
)
from uploaders.azure_blob import init_azure_client


def preview_silver_generated_ohlcv_upload(
    *,
    silver_ohlcv_df: pd.DataFrame,
) -> pd.DataFrame:
    return (
        silver_ohlcv_df[
            [
                "asset",
                "year",
                "month",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "year",
                "month",
                "asset",
            ]
        )
        .reset_index(drop=True)
    )


def upload_silver_generated_ohlcv_from_dataframe(
    *,
    silver_ohlcv_df: pd.DataFrame,
    interval: str = "1m",
    generation_method_id: int = 0,
    generation_method: str = "median_ohlc",
    env_path: Path = Path(".env"),
    data_dir: Path = Path("data"),
    overwrite: bool = False,
    print_progress: bool = True,
) -> pd.DataFrame:
    blob_service_client, container_name = init_azure_client(
        env_path=env_path,
    )

    upload_results = write_and_upload_silver_generated_ohlcv(
        silver_ohlcv_df=silver_ohlcv_df,
        interval=interval,
        generation_method_id=generation_method_id,
        generation_method=generation_method,
        blob_service_client=blob_service_client,
        container_name=container_name,
        data_dir=data_dir,
        overwrite=overwrite,
        print_progress=print_progress,
    )

    return pd.DataFrame(
        [
            result.__dict__
            for result in upload_results
        ]
    )


def upload_silver_generated_ohlcv_with_preview(
    *,
    silver_ohlcv_df: pd.DataFrame,
    interval: str = "1m",
    generation_method_id: int = 0,
    generation_method: str = "median_ohlc",
    env_path: Path = Path(".env"),
    data_dir: Path = Path("data"),
    overwrite: bool = False,
    print_progress: bool = True,
) -> dict[str, pd.DataFrame]:
    preview_df = preview_silver_generated_ohlcv_upload(
        silver_ohlcv_df=silver_ohlcv_df,
    )

    upload_results_df = upload_silver_generated_ohlcv_from_dataframe(
        silver_ohlcv_df=silver_ohlcv_df,
        interval=interval,
        generation_method_id=generation_method_id,
        generation_method=generation_method,
        env_path=env_path,
        data_dir=data_dir,
        overwrite=overwrite,
        print_progress=print_progress,
    )

    return {
        "preview": preview_df,
        "upload_results": upload_results_df,
    }
