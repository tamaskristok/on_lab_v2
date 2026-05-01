from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class BinanceCsvValidationResult:
    file: str
    row_count: int
    bad_column_count: int
    bad_timestamp_count: int
    bad_number_count: int
    duplicate_timestamp_count: int
    is_time_ordered: bool
    min_timestamp: datetime | None
    max_timestamp: datetime | None


def validate_binance_csv(csv_path: Path) -> BinanceCsvValidationResult:
    expected_column_count = 12

    row_count = 0
    bad_column_count = 0
    bad_timestamp_count = 0
    bad_number_count = 0
    duplicate_timestamp_count = 0

    timestamps = set()
    min_timestamp = None
    max_timestamp = None
    previous_timestamp = None
    is_time_ordered = True

    number_columns = [1, 2, 3, 4, 5, 7, 8, 9, 10]

    with open(csv_path, "r", encoding="utf-8") as file:
        reader = csv.reader(file)

        for row in reader:
            row_count += 1

            if len(row) != expected_column_count:
                bad_column_count += 1
                continue

            try:
                timestamp = int(row[0])
                timestamp_dt = datetime.fromtimestamp(
                    timestamp / 1000,
                    tz=timezone.utc,
                )
            except ValueError:
                bad_timestamp_count += 1
                continue

            if timestamp in timestamps:
                duplicate_timestamp_count += 1
            else:
                timestamps.add(timestamp)

            if previous_timestamp is not None and timestamp < previous_timestamp:
                is_time_ordered = False

            previous_timestamp = timestamp

            if min_timestamp is None or timestamp_dt < min_timestamp:
                min_timestamp = timestamp_dt

            if max_timestamp is None or timestamp_dt > max_timestamp:
                max_timestamp = timestamp_dt

            for col_index in number_columns:
                try:
                    float(row[col_index])
                except ValueError:
                    bad_number_count += 1

    return BinanceCsvValidationResult(
        file=str(csv_path),
        row_count=row_count,
        bad_column_count=bad_column_count,
        bad_timestamp_count=bad_timestamp_count,
        bad_number_count=bad_number_count,
        duplicate_timestamp_count=duplicate_timestamp_count,
        is_time_ordered=is_time_ordered,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )
