from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ProjectConfig:
    name: str = "on_lab_v2"


@dataclass
class PathsConfig:
    config_dir: str = "config"
    data_dir: str = "data"
    env_path: str = ".env"


@dataclass
class ConfigFilesConfig:
    broker_strategy: str = "broker_strategy.csv"
    broker_asset_matrix: str = "broker_asset_matrix.csv"
    broker_asset_settings: str = "broker_asset_settings.csv"
    download_period: str = "download_period.csv"
    saxo_bank_instruments: str = "saxo_bank_instruments.csv"
    interactive_brokers_instruments: str = "interactive_brokers_instruments.csv"
    interactive_brokers_calendar_instruments: str = (
        "interactive_brokers_calendar_instruments.csv"
    )
    silver_quality_thresholds: str = "silver_quality_thresholds.csv"


@dataclass
class AzureConfig:
    container_name: str = "market-data"


@dataclass
class RunConfig:
    start_month: str | None = None
    end_month: str | None = None
    interval: str = "1m"
    source: str = "azure"
    print_progress: bool = True


@dataclass
class FiltersConfig:
    brokers: list[str] | None = None
    assets: list[str] | None = None


@dataclass
class SaxoConfig:
    base_url: str = "https://gateway.saxobank.com/sim/openapi"


@dataclass
class InteractiveBrokersConfig:
    host: str = "host.docker.internal"
    port: int = 7497
    client_id: int = 120
    readonly: bool = True
    timeout_sec: int = 30


@dataclass
class SilverConfig:
    generation_methods: list[int] = field(
        default_factory=lambda: [0, 1],
    )
    overwrite: bool = False
    upload: bool = False


@dataclass
class AppConfig:
    project: ProjectConfig = field(
        default_factory=ProjectConfig,
    )
    paths: PathsConfig = field(
        default_factory=PathsConfig,
    )
    config_files: ConfigFilesConfig = field(
        default_factory=ConfigFilesConfig,
    )
    azure: AzureConfig = field(
        default_factory=AzureConfig,
    )
    run: RunConfig = field(
        default_factory=RunConfig,
    )
    filters: FiltersConfig = field(
        default_factory=FiltersConfig,
    )
    saxo: SaxoConfig = field(
        default_factory=SaxoConfig,
    )
    interactive_brokers: InteractiveBrokersConfig = field(
        default_factory=InteractiveBrokersConfig,
    )
    silver: SilverConfig = field(
        default_factory=SilverConfig,
    )
