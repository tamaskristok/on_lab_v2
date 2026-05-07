from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd

from quality.data_loader import load_bronze_ohlcv
from uploaders.azure_blob import init_azure_client


CALENDAR_COLUMNS = [
    "timestamp",
    "asset",
    "year",
    "month",
    "interval",
    "expected",
    "calendar_source",
]


QUALITY_BASE_COLUMNS = [
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
    "expected",
    "is_present",
    "calendar_source",
    "source_timestamp",
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
    start_month: str | None,
    end_month: str | None,
) -> bool:
    key = _month_key(
        year=year,
        month=month,
    )

    if start_month is not None and key < start_month:
        return False

    if end_month is not None and key > end_month:
        return False

    return True


def _read_azure_parquet(
    *,
    container_client,
    blob_name: str,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    blob_client = container_client.get_blob_client(blob_name)
    blob_bytes = blob_client.download_blob().readall()

    return pd.read_parquet(
        BytesIO(blob_bytes),
        columns=columns,
    )


def _empty_calendar_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=CALENDAR_COLUMNS,
    )


def _empty_quality_base_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=QUALITY_BASE_COLUMNS,
    )


def _format_calendar_df(
    *,
    calendar_df: pd.DataFrame,
    asset: str,
    year: int,
    month: int,
    interval: str,
) -> pd.DataFrame:
    formatted_df = calendar_df.copy()

    formatted_df["timestamp"] = pd.to_datetime(
        formatted_df["timestamp"],
        utc=True,
        errors="coerce",
    )

    formatted_df["timestamp"] = formatted_df["timestamp"].dt.floor("min")

    formatted_df["asset"] = asset.upper()
    formatted_df["year"] = year
    formatted_df["month"] = month
    formatted_df["interval"] = interval

    if "expected" not in formatted_df.columns:
        formatted_df["expected"] = True

    if "calendar_source" not in formatted_df.columns:
        formatted_df["calendar_source"] = "unknown"

    formatted_df = formatted_df[
        [
            "timestamp",
            "asset",
            "year",
            "month",
            "interval",
            "expected",
            "calendar_source",
        ]
    ]

    return (
        formatted_df
        .dropna(subset=["timestamp"])
        .drop_duplicates(subset=["timestamp", "asset", "year", "month"])
        .sort_values(["asset", "timestamp"])
        .reset_index(drop=True)
    )


def _parse_calendar_blob_name(
    *,
    blob_name: str,
) -> tuple[str, int, int] | None:
    parts = blob_name.split("/")

    if len(parts) < 6:
        return None

    layer, dataset, asset_folder, year_text, month_text = parts[:5]

    if layer != "silver":
        return None

    if dataset != "calendar":
        return None

    try:
        year = int(year_text)
        month = int(month_text)
    except ValueError:
        return None

    return asset_folder.upper(), year, month


def load_calendar(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    assets: list[str] | str | None = None,
    interval: str = "1m",
    source: str = "azure",
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    data_dir: Path = Path("data"),
    print_progress: bool = False,
) -> pd.DataFrame:
    wanted_assets = _normalize_filter_values(
        assets,
        upper=True,
    )

    source = source.lower()

    if source == "azure":
        return _load_calendar_from_azure(
            start_month=start_month,
            end_month=end_month,
            assets=wanted_assets,
            interval=interval,
            env_path=env_path,
            container_name=container_name,
            print_progress=print_progress,
        )

    if source == "local":
        return _load_calendar_from_local(
            start_month=start_month,
            end_month=end_month,
            assets=wanted_assets,
            interval=interval,
            data_dir=data_dir,
            print_progress=print_progress,
        )

    raise ValueError(f"Unsupported source: {source}")


def _load_calendar_from_azure(
    *,
    start_month: str | None,
    end_month: str | None,
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

    for blob in container_client.list_blobs(name_starts_with="silver/calendar/"):
        blob_name = blob.name

        if not blob_name.endswith(".parquet"):
            continue

        if f"-calendar-{interval}-" not in Path(blob_name).name:
            continue

        parsed = _parse_calendar_blob_name(
            blob_name=blob_name,
        )

        if parsed is None:
            continue

        asset, year, month = parsed

        if assets is not None and asset not in assets:
            continue

        if not _is_in_month_range(
            year=year,
            month=month,
            start_month=start_month,
            end_month=end_month,
        ):
            continue

        if print_progress:
            print(f"reading calendar {blob_name}")

        calendar_df = _read_azure_parquet(
            container_client=container_client,
            blob_name=blob_name,
        )

        frames.append(
            _format_calendar_df(
                calendar_df=calendar_df,
                asset=asset,
                year=year,
                month=month,
                interval=interval,
            )
        )

    if not frames:
        return _empty_calendar_df()

    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["asset", "timestamp"])
        .reset_index(drop=True)
    )


def _load_calendar_from_local(
    *,
    start_month: str | None,
    end_month: str | None,
    assets: set[str] | None,
    interval: str,
    data_dir: Path,
    print_progress: bool,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    calendar_dir = data_dir / "silver" / "calendar"

    for parquet_path in calendar_dir.rglob("*.parquet"):
        if f"-calendar-{interval}-" not in parquet_path.name:
            continue

        relative_parts = list(
            parquet_path.relative_to(data_dir).parts,
        )

        if len(relative_parts) < 6:
            continue

        _, _, asset_folder, year_text, month_text = relative_parts[:5]

        asset = asset_folder.upper()
        year = int(year_text)
        month = int(month_text)

        if assets is not None and asset not in assets:
            continue

        if not _is_in_month_range(
            year=year,
            month=month,
            start_month=start_month,
            end_month=end_month,
        ):
            continue

        if print_progress:
            print(f"reading calendar {parquet_path}")

        calendar_df = pd.read_parquet(parquet_path)

        frames.append(
            _format_calendar_df(
                calendar_df=calendar_df,
                asset=asset,
                year=year,
                month=month,
                interval=interval,
            )
        )

    if not frames:
        return _empty_calendar_df()

    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["asset", "timestamp"])
        .reset_index(drop=True)
    )


def build_calendar_aligned_quality_base(
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
    bronze_df = load_bronze_ohlcv(
        start_month=start_month,
        end_month=end_month,
        brokers=brokers,
        assets=assets,
        interval=interval,
        source=source,
        config_dir=config_dir,
        env_path=env_path,
        container_name=container_name,
        data_dir=data_dir,
        print_progress=print_progress,
    )

    if bronze_df.empty:
        return _empty_quality_base_df()

    calendar_df = load_calendar(
        start_month=start_month,
        end_month=end_month,
        assets=assets,
        interval=interval,
        source=source,
        env_path=env_path,
        container_name=container_name,
        data_dir=data_dir,
        print_progress=print_progress,
    )

    if calendar_df.empty:
        return _empty_quality_base_df()

    working_df = bronze_df.copy()

    working_df["source_timestamp"] = pd.to_datetime(
        working_df["timestamp"],
        utc=True,
        errors="coerce",
    )

    working_df["timestamp"] = working_df["source_timestamp"].dt.floor("min")

    working_df = working_df.dropna(
        subset=["timestamp"],
    )

    calendar_keys_df = calendar_df[
        [
            "timestamp",
            "asset",
            "year",
            "month",
            "expected",
            "calendar_source",
        ]
    ].copy()

    calendar_keys_df["timestamp"] = pd.to_datetime(
        calendar_keys_df["timestamp"],
        utc=True,
        errors="coerce",
    ).dt.floor("min")

    calendar_keys_df = calendar_keys_df.dropna(
        subset=["timestamp"],
    )

    aligned_df = calendar_keys_df.merge(
        working_df,
        on=[
            "timestamp",
            "asset",
            "year",
            "month",
        ],
        how="left",
    )

    aligned_df["is_present"] = aligned_df["broker"].notna()

    return (
        aligned_df[QUALITY_BASE_COLUMNS]
        .sort_values(
            [
                "asset",
                "broker",
                "timestamp",
            ],
            na_position="last",
        )
        .reset_index(drop=True)
    )
