from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


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


def _read_parquet_blob(
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


def _azure_blob_exists(
    *,
    container_client,
    blob_name: str,
) -> bool:
    return container_client.get_blob_client(blob_name).exists()


def _init_azure_container_client(
    *,
    env_path: Path,
    container_name: str,
):
    load_dotenv(env_path)

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not connection_string:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string,
    )

    return blob_service_client.get_container_client(
        container_name,
    )


def _empty_broker_ticker_overview() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "broker",
            "ticker",
            "months",
            "rows",
            "expected_rows",
            "coverage_pct",
            "start",
            "end",
            "duplicate_timestamps",
        ]
    )


def _build_broker_ticker_overview_from_records(
    *,
    records: list[dict],
) -> pd.DataFrame:
    if not records:
        return _empty_broker_ticker_overview()

    monthly_df = pd.DataFrame(records)

    overview_df = (
        monthly_df
        .groupby(["broker", "ticker"], as_index=False)
        .agg(
            months=("month", "count"),
            rows=("rows", "sum"),
            expected_rows=("expected_rows", "sum"),
            start=("start", "min"),
            end=("end", "max"),
            duplicate_timestamps=("duplicate_timestamps", "sum"),
        )
        .sort_values(["ticker", "broker"])
        .reset_index(drop=True)
    )

    overview_df["coverage_pct"] = (
        overview_df["rows"] / overview_df["expected_rows"] * 100
    ).round(2)

    overview_df.loc[
        overview_df["expected_rows"].isna()
        | (overview_df["expected_rows"] == 0),
        "coverage_pct",
    ] = pd.NA

    return overview_df[
        [
            "broker",
            "ticker",
            "months",
            "rows",
            "expected_rows",
            "coverage_pct",
            "start",
            "end",
            "duplicate_timestamps",
        ]
    ]


def _collect_broker_ticker_records_from_azure(
    *,
    start_month: str | None,
    end_month: str | None,
    brokers: list[str] | None,
    tickers: list[str] | None,
    interval: str,
    env_path: Path,
    container_name: str,
    print_progress: bool,
) -> list[dict]:
    container_client = _init_azure_container_client(
        env_path=env_path,
        container_name=container_name,
    )

    wanted_brokers = (
        {broker.lower() for broker in brokers}
        if brokers is not None
        else None
    )

    wanted_tickers = (
        {ticker.upper() for ticker in tickers}
        if tickers is not None
        else None
    )

    records = []

    for blob in container_client.list_blobs(name_starts_with="bronze/"):
        blob_name = blob.name

        if not blob_name.endswith(".parquet"):
            continue

        parts = blob_name.split("/")

        if len(parts) < 6:
            continue

        _, broker, ticker_folder, year_text, month_text, _ = parts[:6]

        ticker = ticker_folder.upper()
        year = int(year_text)
        month = int(month_text)

        if not _is_in_month_range(
            year=year,
            month=month,
            start_month=start_month,
            end_month=end_month,
        ):
            continue

        if wanted_brokers is not None and broker.lower() not in wanted_brokers:
            continue

        if wanted_tickers is not None and ticker not in wanted_tickers:
            continue

        if print_progress:
            print(f"reading {blob_name}")

        data_df = _read_parquet_blob(
            container_client=container_client,
            blob_name=blob_name,
            columns=["timestamp"],
        )

        data_df["timestamp"] = pd.to_datetime(
            data_df["timestamp"],
            utc=True,
            errors="coerce",
        )

        calendar_blob_name = (
            f"silver/calendar/{ticker.lower()}/{year}/{month:02d}/"
            f"{ticker}-calendar-{interval}-{year}-{month:02d}.parquet"
        )

        expected_rows = None
        has_calendar = False
        calendar_filtered_rows = None
        calendar_filtered_start = None
        calendar_filtered_end = None
        calendar_filtered_duplicate_timestamps = None

        if _azure_blob_exists(
            container_client=container_client,
            blob_name=calendar_blob_name,
        ):
            calendar_df = _read_parquet_blob(
                container_client=container_client,
                blob_name=calendar_blob_name,
                columns=["timestamp"],
            )

            calendar_df["timestamp"] = pd.to_datetime(
                calendar_df["timestamp"],
                utc=True,
                errors="coerce",
            )

            expected_rows = len(calendar_df)
            has_calendar = True

            calendar_timestamps_df = calendar_df.drop_duplicates(
                subset=["timestamp"],
            )[["timestamp"]]

            calendar_filtered_df = data_df.merge(
                calendar_timestamps_df,
                on="timestamp",
                how="inner",
            )

            calendar_filtered_rows = len(calendar_filtered_df)
            calendar_filtered_start = calendar_filtered_df["timestamp"].min()
            calendar_filtered_end = calendar_filtered_df["timestamp"].max()
            calendar_filtered_duplicate_timestamps = int(
                calendar_filtered_df["timestamp"].duplicated().sum()
            )

        records.append(
            {
                "broker": broker,
                "ticker": ticker,
                "year": year,
                "month": month,
                "rows": len(data_df),
                "expected_rows": expected_rows,
                "start": data_df["timestamp"].min(),
                "end": data_df["timestamp"].max(),
                "duplicate_timestamps": int(
                    data_df["timestamp"].duplicated().sum()
                ),
                "has_calendar": has_calendar,
                "calendar_filtered_rows": calendar_filtered_rows,
                "calendar_filtered_start": calendar_filtered_start,
                "calendar_filtered_end": calendar_filtered_end,
                "calendar_filtered_duplicate_timestamps": (
                    calendar_filtered_duplicate_timestamps
                ),
            }
        )

    return records


def build_broker_ticker_report_tables_from_azure(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | None = None,
    tickers: list[str] | None = None,
    interval: str = "1m",
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    print_progress: bool = False,
) -> dict[str, pd.DataFrame]:
    records = _collect_broker_ticker_records_from_azure(
        start_month=start_month,
        end_month=end_month,
        brokers=brokers,
        tickers=tickers,
        interval=interval,
        env_path=env_path,
        container_name=container_name,
        print_progress=print_progress,
    )

    overview_df = _build_broker_ticker_overview_from_records(
        records=records,
    )

    calendar_records = []

    for record in records:
        if not record["has_calendar"]:
            continue

        calendar_records.append(
            {
                "broker": record["broker"],
                "ticker": record["ticker"],
                "year": record["year"],
                "month": record["month"],
                "rows": record["calendar_filtered_rows"],
                "expected_rows": record["expected_rows"],
                "start": record["calendar_filtered_start"],
                "end": record["calendar_filtered_end"],
                "duplicate_timestamps": (
                    record["calendar_filtered_duplicate_timestamps"]
                ),
            }
        )

    calendar_filtered_df = _build_broker_ticker_overview_from_records(
        records=calendar_records,
    )

    return {
        "overview": overview_df,
        "calendar_filtered": calendar_filtered_df,
    }


def build_broker_ticker_overview_from_azure(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | None = None,
    tickers: list[str] | None = None,
    interval: str = "1m",
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    print_progress: bool = False,
) -> pd.DataFrame:
    tables = build_broker_ticker_report_tables_from_azure(
        start_month=start_month,
        end_month=end_month,
        brokers=brokers,
        tickers=tickers,
        interval=interval,
        env_path=env_path,
        container_name=container_name,
        print_progress=print_progress,
    )

    return tables["overview"]


def build_calendar_filtered_broker_ticker_overview_from_azure(
    *,
    start_month: str | None = None,
    end_month: str | None = None,
    brokers: list[str] | None = None,
    tickers: list[str] | None = None,
    interval: str = "1m",
    env_path: Path = Path(".env"),
    container_name: str = "market-data",
    print_progress: bool = False,
) -> pd.DataFrame:
    tables = build_broker_ticker_report_tables_from_azure(
        start_month=start_month,
        end_month=end_month,
        brokers=brokers,
        tickers=tickers,
        interval=interval,
        env_path=env_path,
        container_name=container_name,
        print_progress=print_progress,
    )

    return tables["calendar_filtered"]
