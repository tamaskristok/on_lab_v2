from __future__ import annotations

import os
from pathlib import Path

import hydra
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from config_loader.hydra_config import AppConfig
from pipelines.monthly_ingestion_runner import run_monthly_ingestion_from_config
from validators.config_files import validate_config_files


SAXO_ACCESS_TOKEN_ENV_NAME = "SAXO_ACCESS_TOKEN"


cs = ConfigStore.instance()
cs.store(
    name="config_schema",
    node=AppConfig,
)


def _requires_broker(
    brokers: list[str] | None,
    broker: str,
) -> bool:
    return brokers is None or broker in brokers


def _validate_config(cfg: AppConfig) -> None:
    config_files = OmegaConf.to_container(
        cfg.config_files,
        resolve=True,
    )

    validate_config_files(
        config_dir=Path(cfg.paths.config_dir),
        config_files=config_files,
        print_progress=True,
    )


@hydra.main(
    version_base=None,
    config_path="../conf",
    config_name="config",
)
def main(cfg: AppConfig) -> None:
    print("Ingestion pipeline config:")
    print(OmegaConf.to_yaml(cfg, resolve=True))

    print("Config validation indul")
    _validate_config(cfg)
    print("Config validation kesz")

    saxo_access_token = None

    if _requires_broker(
        brokers=cfg.filters.brokers,
        broker="saxo_bank",
    ):
        saxo_access_token = os.getenv(SAXO_ACCESS_TOKEN_ENV_NAME)

        if not saxo_access_token:
            raise ValueError(
                "SAXO_ACCESS_TOKEN environment variable is required "
                "when running saxo_bank"
            )

    results_df = run_monthly_ingestion_from_config(
        start_month=cfg.run.start_month,
        end_month=cfg.run.end_month,
        brokers=cfg.filters.brokers,
        assets=cfg.filters.assets,
        interval=cfg.run.interval,
        config_dir=Path(cfg.paths.config_dir),
        env_path=Path(cfg.paths.env_path),
        data_dir=Path(cfg.paths.data_dir),
        saxo_access_token=saxo_access_token,
        saxo_base_url=cfg.saxo.base_url,
        saxo_print_progress=cfg.run.print_progress,
        interactive_brokers_host=cfg.interactive_brokers.host,
        interactive_brokers_port=cfg.interactive_brokers.port,
        interactive_brokers_client_id=cfg.interactive_brokers.client_id,
        interactive_brokers_readonly=cfg.interactive_brokers.readonly,
        interactive_brokers_timeout_sec=cfg.interactive_brokers.timeout_sec,
        interactive_brokers_print_progress=cfg.run.print_progress,
        dukascopy_print_progress=cfg.run.print_progress,
    )

    print("Ingestion pipeline kesz")
    print(results_df)


if __name__ == "__main__":
    main()
