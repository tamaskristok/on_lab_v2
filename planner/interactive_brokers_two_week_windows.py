from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone

from planner.time_windows import TimeWindow


@dataclass(frozen=True)
class InteractiveBrokersTwoWeekWindow:
    end_datetime: datetime
    duration: str


def generate_interactive_brokers_two_week_windows(
    window: TimeWindow,
) -> list[InteractiveBrokersTwoWeekWindow]:
    month_end_exclusive = window.end_date + timedelta(days=1)

    end_dates = [
        window.start_date + timedelta(days=14),
        window.start_date + timedelta(days=28),
        month_end_exclusive,
    ]

    unique_end_dates = sorted(set(end_dates))

    return [
        InteractiveBrokersTwoWeekWindow(
            end_datetime=datetime.combine(
                end_date,
                time.min,
                tzinfo=timezone.utc,
            ),
            duration="2 W",
        )
        for end_date in unique_end_dates
    ]
