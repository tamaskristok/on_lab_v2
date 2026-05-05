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
    windows: list[InteractiveBrokersTwoWeekWindow] = []

    first_end_date = min(
        window.start_date.replace(day=15),
        window.end_date + timedelta(days=1),
    )

    windows.append(
        InteractiveBrokersTwoWeekWindow(
            end_datetime=datetime.combine(
                first_end_date,
                time.min,
                tzinfo=timezone.utc,
            ),
            duration="2 W",
        )
    )

    windows.append(
        InteractiveBrokersTwoWeekWindow(
            end_datetime=datetime.combine(
                window.end_date + timedelta(days=1),
                time.min,
                tzinfo=timezone.utc,
            ),
            duration="2 W",
        )
    )

    return windows
