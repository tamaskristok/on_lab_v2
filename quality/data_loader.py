from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd

from config_loader.csv_config import load_download_period
from uploaders.azure_blob import init_azure_client


BRONZE_COLUMNS = [
    "timestamp",
    "asset",
    "broker",
    "year",
    "month",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

OHLCV_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def _normalize_filter_values(
    values: list[str] | str | None,
    *,
    upper: bool,
) -> set[str] | None:
    if values is None:
        return None

    if isinstance(values, str):
        values = [values]

    normalized_values = {
        value.strip()
        for value in values
    }

    if upper:
        return {
            value.upper()
            for value in normalized_values
        }

    return {
        value.lower()
        for value in normalized_values
    }


def _parse_month(
    month_text: str,
) -> tuple[int, int]:
    year_text, month_number_text = month_text.split("-")
    return int(year_text), int(month_number_text)


def _month_key(
    *,
    year: int,
    month: int,
) -> str:
    return f"{year}-{month:02d}"


def _is_in_month_range(
    *,
    year: int,
    month: int,
    start_month: str,
    end_month: str,
) -> bool:
    key = _month_key(
        year=year,
        month=month,
    )

    return start_month <= key <= end_month


def _load_month_range_from_config(
    *,
    config_dir: Path,
) -> tuple[str, str]:
    download_period_df = load_download_period(config_dir)
    period = download_period_df.iloc[0]

    start_date = pd.to_datetime(
        period["start_date"],
    )
    end_date = pd.to_datetime(
        period["end_date"],
    )

    return (
        f"{start_date.year}-{start_date.month:02d}",
        f"{end_date.year}-{end_date.month:02d}",
    )


def _resolve_month_range(
    *,
    start_month: str | None,
    end_month: str | None,
    config_dir: Path,
) -> tuple[str, str]:
    if start_month is None and end_month is None:
        return _load_month_range_from_config(
            config_dir=config_dir,
        )

    if start_month is None or end_month is None:
        raise ValueError(
            "start_month and end_month must both be set, or both be None."
        )

    _parse_month(start_month)
    _parse_month(end_month)

    if start_month > end_month:
        raise ValueError("start_month cannot be after end_month.")

    return start_month, end_month


def _empty_bronze_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=BRONZE_COLUMNS,
    )


def _parse_bronze_path_parts(
    *,
    path_parts: list[str],
) -> tuple[str, str, int, int] | None:
    if len(path_parts) < 6:
        return None

    layer, broker, asset, year_text, month_text = path_parts[:5]

    if layer != "bronze":
        return None

    return (
        broker,
        asset.upper(),
        int(year_text),
        int(month_text),
    )


def _read_local_parquet(
    *,
    parquet_path: Path,
) -> pd.DataFrame:
    return pd.read_parquet(
        parquet_path,
        columns=OHLCV_COLUMNS,
    )


def _read_azure_parquet(
    *,
    container_client,
    blob_name: str,
) -> pd.DataFrame:
    blob_client = container_client.get_blob_client(blob_name)
    blob_bytes = blob_client.download_blob().readall()

    return pd.read_parquet(
        BytesIO(blob_bytes),
        columns=OHLCV_COLUMNS,
    )


def _format_bronze_df(
    *,
    df: pd.DataFrame,
    broker: str,
    asset: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    formatted_df = df.copy()

    formatted_df["timestamp"] = pd.to_datetime(
        formatted_df["timestamp"],
        utc=True,
        errors="coerce",
    )

    formatted_df["asset"] = asset.upper()
    formatted_df["broker"] = broker
    formatted_df["year"] = year
    formatted_df["month"] = month

    return formatted_df[BRONZE_COLUMNS]


def load_bronze_ohlcv(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | str | None = None,
    assets: list[str] | str | None = None,
    interval: str = "1m",
    source: str = "azure",
    config_dir: Path = Path("config"),
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    data_dir: Path = Path("data"),
    print_progress: bool = False,
) -> pd.DataFrame:
    start_month, end_month = _resolve_month_range(
        start_month=start_month,
        end_month=end_month,
        config_dir=config_dir,
    )

    wanted_brokers = _normalize_filter_values(
        brokers,
        upper=False,
    )
    wanted_assets = _normalize_filter_values(
        assets,
        upper=True,
    )

    source = source.lower()

    if source == "azure":
        return _load_bronze_ohlcv_from_azure(
            start_month=start_month,
            end_month=end_month,
            brokers=wanted_brokers,
            assets=wanted_assets,
            interval=interval,
            env_path=env_path,
            container_name=container_name,
            print_progress=print_progress,
        )

    if source == "local":
        return _load_bronze_ohlcv_from_local(
            start_month=start_month,
            end_month=end_month,
            brokers=wanted_brokers,
            assets=wanted_assets,
            interval=interval,
            data_dir=data_dir,
            print_progress=print_progress,
        )

    raise ValueError(f"Unsupported source: {source}")


def _load_bronze_ohlcv_from_azure(
    *,
    start_month: str,
    end_month: str,
    brokers: set[str] | None,
    assets: set[str] | None,
    interval: str,
    env_path: Path,
    container_name: str,
    print_progress: bool,
) -> pd.DataFrame:
    blob_service_client, resolved_container_name = init_azure_client(
        env_path=env_path,
        container_name=container_name,
    )
    container_client = blob_service_client.get_container_client(
        resolved_container_name,
    )

    frames: list[pd.DataFrame] = []

    for blob in container_client.list_blobs(name_starts_with="bronze/"):
        blob_name = blob.name

        if not blob_name.endswith(".parquet"):
            continue

        if f"-{interval}-" not in Path(blob_name).name:
            continue

        parsed_parts = _parse_bronze_path_parts(
            path_parts=blob_name.split("/"),
        )

        if parsed_parts is None:
            continue

        broker, asset, year, month = parsed_parts

        if not _is_in_month_range(
            year=year,
            month=month,
            start_month=start_month,
            end_month=end_month,
        ):
            continue

        if brokers is not None and broker.lower() not in brokers:
            continue

        if assets is not None and asset.upper() not in assets:
            continue

        if print_progress:
            print(f"reading {blob_name}")

        df = _read_azure_parquet(
            container_client=container_client,
            blob_name=blob_name,
        )

        frames.append(
            _format_bronze_df(
                df=df,
                broker=broker,
                asset=asset,
                year=year,
                month=month,
            )
        )

    return _combine_bronze_frames(
        frames=frames,
    )


def _load_bronze_ohlcv_from_local(
    *,
    start_month: str,
    end_month: str,
    brokers: set[str] | None,
    assets: set[str] | None,
    interval: str,
    data_dir: Path,
    print_progress: bool,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    bronze_dir = data_dir / "bronze"

    for parquet_path in bronze_dir.rglob("*.parquet"):
        if f"-{interval}-" not in parquet_path.name:
            continue

        relative_parts = list(
            parquet_path.relative_to(data_dir).parts,
        )

        parsed_parts = _parse_bronze_path_parts(
            path_parts=relative_parts,
        )

        if parsed_parts is None:
            continue

        broker, asset, year, month = parsed_parts

        if not _is_in_month_range(
            year=year,
            month=month,
            start_month=start_month,
            end_month=end_month,
        ):
            continue

        if brokers is not None and broker.lower() not in brokers:
            continue

        if assets is not None and asset.upper() not in assets:
            continue

        if print_progress:
            print(f"reading {parquet_path}")

        df = _read_local_parquet(
            parquet_path=parquet_path,
        )

        frames.append(
            _format_bronze_df(
                df=df,
                broker=broker,
                asset=asset,
                year=year,
                month=month,
            )
        )

    return _combine_bronze_frames(
        frames=frames,
    )


def _combine_bronze_frames(
    *,
    frames: list[pd.DataFrame],
) -> pd.DataFrame:
    if not frames:
        return _empty_bronze_df()

    return (
        pd.concat(
            frames,
            ignore_index=True,
        )
        .sort_values(
            [
                "asset",
                "timestamp",
                "broker",
            ]
        )
        .reset_index(drop=True)
    )
