from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path

import requests


@dataclass(frozen=True)
class BinanceRequest:
    symbol: str
    interval: str
    year: int
    month: int


@dataclass(frozen=True)
class BinanceDownloadResult:
    url: str
    files: list[Path]


class BinanceProvider:
    base_url = "https://data.binance.vision"

    def __init__(self, timeout_sec: int = 30):
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def build_url(self, request: BinanceRequest) -> str:
        month = f"{request.month:02d}"

        return (
            f"{self.base_url}/data/spot/monthly/klines/"
            f"{request.symbol}/{request.interval}/"
            f"{request.symbol}-{request.interval}-{request.year}-{month}.zip"
        )

    def download(self, request: BinanceRequest, output_dir: Path) -> BinanceDownloadResult:
        url = self.build_url(request)

        response = self.session.get(url, timeout=self.timeout_sec)

        if response.status_code == 404:
            raise FileNotFoundError(f"Binance file not found: {url}")

        if response.status_code != 200:
            raise RuntimeError(f"Binance HTTP error {response.status_code}: {url}")

        files = self._extract_csv_files(
            zip_bytes=response.content,
            output_dir=output_dir,
        )

        return BinanceDownloadResult(
            url=url,
            files=files,
        )

    def _extract_csv_files(self, zip_bytes: bytes, output_dir: Path) -> list[Path]:
        output_dir.mkdir(parents=True, exist_ok=True)

        extracted_files: list[Path] = []

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            for member in archive.infolist():
                if not member.filename.lower().endswith(".csv"):
                    continue

                member_path = Path(member.filename)

                if member_path.is_absolute() or ".." in member_path.parts:
                    raise ValueError(f"Unsafe ZIP entry: {member.filename}")

                archive.extract(member, output_dir)
                extracted_files.append(output_dir / member.filename)

        if not extracted_files:
            raise ValueError("No CSV file found in Binance ZIP.")

        return extracted_files
