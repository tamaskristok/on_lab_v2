from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ib_insync import IB, Contract, util


@dataclass(frozen=True)
class InteractiveBrokersHistoricalRequest:
    con_id: int
    symbol: str
    security_type: str
    exchange: str
    currency: str
    end_datetime: datetime
    duration: str
    bar_size: str
    what_to_show: str
    use_regular_trading_hours: bool


@dataclass(frozen=True)
class InteractiveBrokersHistoricalResult:
    data: list[dict]


class InteractiveBrokersProvider:
    def __init__(
        self,
        *,
        host: str = "host.docker.internal",
        port: int = 7497,
        client_id: int = 10,
        readonly: bool = True,
        timeout: int = 10,
    ) -> None:
        self.ib = IB()
        self.ib.connect(
            host=host,
            port=port,
            clientId=client_id,
            readonly=readonly,
            timeout=timeout,
        )

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()

    def download_historical_bars(
        self,
        request: InteractiveBrokersHistoricalRequest,
    ) -> InteractiveBrokersHistoricalResult:
        contract = Contract(
            conId=request.con_id,
            symbol=request.symbol,
            secType=request.security_type,
            exchange=request.exchange,
            currency=request.currency,
        )

        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime=request.end_datetime,
            durationStr=request.duration,
            barSizeSetting=request.bar_size,
            whatToShow=request.what_to_show,
            useRTH=request.use_regular_trading_hours,
            formatDate=2,
        )

        df = util.df(bars)

        if df is None:
            return InteractiveBrokersHistoricalResult(data=[])

        return InteractiveBrokersHistoricalResult(
            data=df.to_dict(orient="records"),
        )
