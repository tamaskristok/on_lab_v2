from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests


@dataclass(frozen=True)
class DukascopyRequest:
    symbol: str
    hour_start: datetime


@dataclass(frozen=True)
class DukascopyDownloadResult:
    url: str
    file: Path
    hour_start: datetime
    is_empty: bool


class DukascopyProvider:
    base_url = "https://datafeed.dukascopy.com/datafeed"

    def __init__(self, timeout_sec: int = 30):
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def build_url(self, request: DukascopyRequest) -> str:
        hour_start = request.hour_start
        dukascopy_month = hour_start.month - 1

        return (
            f"{self.base_url}/"
            f"{request.symbol}/"
            f"{hour_start.year}/"
            f"{dukascopy_month:02d}/"
            f"{hour_start.day:02d}/"
            f"{hour_start.hour:02d}h_ticks.bi5"
        )

    def download(
        self,
        request: DukascopyRequest,
        output_dir: Path,
    ) -> DukascopyDownloadResult:
        url = self.build_url(request)

        response = self.session.get(
            url,
            timeout=self.timeout_sec,
        )

        if response.status_code == 404:
            raise FileNotFoundError(f"Dukascopy file not found: {url}")

        if response.status_code != 200:
            raise RuntimeError(f"Dukascopy HTTP error {response.status_code}: {url}")

        output_dir.mkdir(parents=True, exist_ok=True)

        file_name = (
            f"{request.symbol}-"
            f"{request.hour_start:%Y-%m-%d-%H}"
            "_ticks.bi5"
        )

        output_path = output_dir / file_name
        output_path.write_bytes(response.content)

        return DukascopyDownloadResult(
            url=url,
            file=output_path,
            hour_start=request.hour_start,
            is_empty=len(response.content) == 0,
        )
