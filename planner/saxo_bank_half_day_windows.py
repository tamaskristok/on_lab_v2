from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone

from planner.time_windows import TimeWindow


@dataclass(frozen=True)
class SaxoBankHalfDayWindow:
    start_time: datetime
    end_time: datetime
    count: int


def generate_saxo_bank_half_day_windows(
    window: TimeWindow,
) -> list[SaxoBankHalfDayWindow]:
    windows: list[SaxoBankHalfDayWindow] = []

    current_day = datetime.combine(
        window.start_date,
        time(0, 0),
        tzinfo=timezone.utc,
    )

    end_day = datetime.combine(
        window.end_date,
        time(0, 0),
        tzinfo=timezone.utc,
    )

    while current_day <= end_day:
        next_day = current_day + timedelta(days=1)

        windows.append(
            SaxoBankHalfDayWindow(
                start_time=current_day,
                end_time=current_day + timedelta(hours=13),
                count=13 * 60,
            )
        )

        windows.append(
            SaxoBankHalfDayWindow(
                start_time=current_day + timedelta(hours=12),
                end_time=next_day,
                count=12 * 60,
            )
        )

        current_day = next_day

    return windows
