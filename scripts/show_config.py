from __future__ import annotations

import hydra
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from config_loader.hydra_config import AppConfig


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
    print(OmegaConf.to_yaml(cfg, resolve=True))


if __name__ == "__main__":
    main()
