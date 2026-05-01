from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class TimeWindow:
    start_date: date
    end_date: date


def generate_monthly_windows(
    start_date: date,
    end_date: date,
) -> list[TimeWindow]:
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    windows: list[TimeWindow] = []

    current_start = start_date

    while current_start <= end_date:
        if current_start.month == 12:
            next_month_start = date(current_start.year + 1, 1, 1)
        else:
            next_month_start = date(current_start.year, current_start.month + 1, 1)

        current_end = min(
            next_month_start - timedelta(days=1),
            end_date,
        )

        windows.append(
            TimeWindow(
                start_date=current_start,
                end_date=current_end,
            )
        )

        current_start = next_month_start

    return windows


def generate_daily_windows(
    start_date: date,
    end_date: date,
) -> list[TimeWindow]:
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    windows: list[TimeWindow] = []

    current_date = start_date

    while current_date <= end_date:
        windows.append(
            TimeWindow(
                start_date=current_date,
                end_date=current_date,
            )
        )

        current_date += timedelta(days=1)

    return windows
