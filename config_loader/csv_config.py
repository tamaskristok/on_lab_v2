from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_broker_strategy(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "broker_strategy.csv",
        comment="#",
    )


def load_broker_asset_matrix(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "broker_asset_matrix.csv",
        comment="#",
    )


def load_download_period(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "download_period.csv",
        comment="#",
    )


def load_broker_asset_settings(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "broker_asset_settings.csv",
        comment="#",
    )


def load_saxo_bank_instruments(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "saxo_bank_instruments.csv",
        comment="#",
    )


def load_interactive_brokers_instruments(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "interactive_brokers_instruments.csv",
        comment="#",
    )


def load_interactive_brokers_connection(config_dir: Path) -> pd.DataFrame:
    return pd.read_csv(
        config_dir / "interactive_brokers_connection.csv",
        comment="#",
    )
