from __future__ import annotations

from pathlib import Path

import hydra
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from config_loader.hydra_config import AppConfig
from pipelines.silver_generated_ohlcv_runner import (
    upload_silver_generated_ohlcv_from_dataframe,
)
from pipelines.silver_quality_runner import run_silver_quality_from_config
from validators.config_files import validate_config_files


cs = ConfigStore.instance()
cs.store(
    name="config_schema",
    node=AppConfig,
)


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
    print("Silver quality pipeline config:")
    print(OmegaConf.to_yaml(cfg, resolve=True))

    print("Config validation indul")
    _validate_config(cfg)
    print("Config validation kesz")

    for generation_method in cfg.silver.generation_methods:
        print()
        print(f"=== Silver quality method {generation_method} indul ===")

        result = run_silver_quality_from_config(
            start_month=cfg.run.start_month,
            end_month=cfg.run.end_month,
            brokers=cfg.filters.brokers,
            assets=cfg.filters.assets,
            interval=cfg.run.interval,
            generation_method=generation_method,
            source=cfg.run.source,
            config_dir=Path(cfg.paths.config_dir),
            env_path=Path(cfg.paths.env_path),
            data_dir=Path(cfg.paths.data_dir),
            print_progress=cfg.run.print_progress,
            show_figures=False,
        )

        silver_ohlcv_df = result["silver_ohlcv"]
        summary_df = result["silver_summary"]

        print(f"method {generation_method} kesz")
        print(summary_df)

        if cfg.silver.upload:
            generation_method_name = (
                silver_ohlcv_df["generation_method"]
                .dropna()
                .iloc[0]
            )

            print(f"method {generation_method} feltoltes indul")

            upload_results_df = upload_silver_generated_ohlcv_from_dataframe(
                silver_ohlcv_df=silver_ohlcv_df,
                interval=cfg.run.interval,
                generation_method_id=generation_method,
                generation_method=generation_method_name,
                env_path=Path(cfg.paths.env_path),
                data_dir=Path(cfg.paths.data_dir),
                overwrite=cfg.silver.overwrite,
                print_progress=cfg.run.print_progress,
            )

            print(f"method {generation_method} feltoltes kesz")
            print(upload_results_df)


if __name__ == "__main__":
    main()
