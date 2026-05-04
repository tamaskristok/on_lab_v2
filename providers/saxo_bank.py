from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import requests


@dataclass(frozen=True)
class SaxoChartRequest:
    uic: int
    asset_type: str
    horizon: int
    start_time: datetime
    count: int


@dataclass(frozen=True)
class SaxoChartResult:
    request: SaxoChartRequest
    data: list[dict]


class SaxoBankProvider:
    def __init__(
        self,
        *,
        access_token: str,
        base_url: str = "https://gateway.saxobank.com/sim/openapi",
        timeout_sec: int = 30,
    ):
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def download_chart(
        self,
        request: SaxoChartRequest,
    ) -> SaxoChartResult:
        params = {
            "AssetType": request.asset_type,
            "Uic": request.uic,
            "Horizon": request.horizon,
            "Mode": "From",
            "Time": request.start_time.isoformat().replace("+00:00", "Z"),
            "Count": request.count,
        }

        response = self.session.get(
            f"{self.base_url}/chart/v3/charts",
            headers=self._headers(),
            params=params,
            timeout=self.timeout_sec,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Saxo chart HTTP error {response.status_code}: {response.text[:500]}"
            )

        payload = response.json()

        return SaxoChartResult(
            request=request,
            data=payload.get("Data", []),
        )

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
        }
