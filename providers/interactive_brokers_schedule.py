from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ib_insync import Contract

from providers.interactive_brokers import InteractiveBrokersProvider


@dataclass(frozen=True)
class InteractiveBrokersScheduleRequest:
    con_id: int
    symbol: str
    security_type: str
    exchange: str
    currency: str
    end_datetime: datetime
    num_days: int
    use_regular_trading_hours: bool


@dataclass(frozen=True)
class InteractiveBrokersScheduleSession:
    start_datetime: str
    end_datetime: str
    ref_date: str


@dataclass(frozen=True)
class InteractiveBrokersScheduleResult:
    sessions: list[InteractiveBrokersScheduleSession]


def download_interactive_brokers_schedule(
    *,
    provider: InteractiveBrokersProvider,
    request: InteractiveBrokersScheduleRequest,
) -> InteractiveBrokersScheduleResult:
    contract = Contract(
        conId=request.con_id,
        symbol=request.symbol,
        secType=request.security_type,
        exchange=request.exchange,
        currency=request.currency,
    )

    schedule = provider.ib.reqHistoricalSchedule(
        contract=contract,
        numDays=request.num_days,
        endDateTime=request.end_datetime,
        useRTH=request.use_regular_trading_hours,
    )

    sessions = [
        InteractiveBrokersScheduleSession(
            start_datetime=session.startDateTime,
            end_datetime=session.endDateTime,
            ref_date=session.refDate,
        )
        for session in schedule.sessions
    ]

    return InteractiveBrokersScheduleResult(
        sessions=sessions,
    )
