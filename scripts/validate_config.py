from __future__ import annotations

from pathlib import Path

import hydra
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from config_loader.hydra_config import AppConfig
from validators.config_files import validate_config_files


cs = ConfigStore.instance()
cs.store(
    name="config_schema",
    node=AppConfig,
)


@hydra.main(
    version_base=None,
    config_path="../conf",
    config_name="config",
)
def main(cfg: AppConfig) -> None:
    print("Config validation indul")
    print(OmegaConf.to_yaml(cfg, resolve=True))

    config_files = OmegaConf.to_container(
        cfg.config_files,
        resolve=True,
    )

    validate_config_files(
        config_dir=Path(cfg.paths.config_dir),
        config_files=config_files,
        print_progress=True,
    )

    print("Config validation kesz")


if __name__ == "__main__":
    main()
