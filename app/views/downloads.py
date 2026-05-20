from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pandas as pd
import streamlit as st
from hydra import compose, initialize_config_dir
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from config_loader.hydra_config import AppConfig
from validators.config_files import validate_config_files


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
HYDRA_CONFIG_DIR = PROJECT_ROOT / "conf"
BROKER_ASSET_MATRIX_PATH = CONFIG_DIR / "broker_asset_matrix.csv"


cs = ConfigStore.instance()

try:
    cs.store(
        name="config_schema",
        node=AppConfig,
    )
except ValueError:
    pass


@st.cache_resource
def load_hydra_config() -> AppConfig:
    with initialize_config_dir(
        version_base=None,
        config_dir=str(HYDRA_CONFIG_DIR),
    ):
        return compose(
            config_name="config",
        )


@st.cache_data
def load_broker_asset_matrix() -> pd.DataFrame:
    return pd.read_csv(BROKER_ASSET_MATRIX_PATH)


def ensure_event_loop() -> None:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(
            asyncio.new_event_loop()
        )


def validate_download_config(
    cfg: AppConfig,
) -> None:
    config_files = OmegaConf.to_container(
        cfg.config_files,
        resolve=True,
    )

    validate_config_files(
        config_dir=PROJECT_ROOT / Path(cfg.paths.config_dir),
        config_files=config_files,
        print_progress=False,
    )


def validate_month_range(
    *,
    start_month: str,
    end_month: str,
) -> None:
    month_pattern = re.compile(r"^\d{4}-\d{2}$")

    if not month_pattern.match(start_month):
        raise ValueError("A kezdő hónap formátuma legyen YYYY-MM.")

    if not month_pattern.match(end_month):
        raise ValueError("A záró hónap formátuma legyen YYYY-MM.")

    start_period = pd.Period(
        start_month,
        freq="M",
    )
    end_period = pd.Period(
        end_month,
        freq="M",
    )

    current_period = pd.Timestamp.today().to_period("M")

    if start_period > end_period:
        raise ValueError("A kezdő hónap nem lehet későbbi, mint a záró hónap.")

    if end_period >= current_period:
        raise ValueError("Csak teljesen lezárt múltbeli hónap kérhető le.")


def get_brokers(
    broker_asset_matrix_df: pd.DataFrame,
) -> list[str]:
    return (
        broker_asset_matrix_df["broker"]
        .dropna()
        .astype(str)
        .sort_values()
        .tolist()
    )


def get_assets(
    broker_asset_matrix_df: pd.DataFrame,
    selected_brokers: list[str],
) -> list[str]:
    if not selected_brokers:
        return []

    asset_columns = [
        column
        for column in broker_asset_matrix_df.columns
        if column != "broker"
    ]

    selected_rows = broker_asset_matrix_df[
        broker_asset_matrix_df["broker"].isin(selected_brokers)
    ]

    available_assets = []

    for asset in asset_columns:
        values = selected_rows[asset].dropna().astype(str)

        if (values != "NOK").any():
            available_assets.append(asset)

    return available_assets


cfg = load_hydra_config()

st.title("Bróker adatok letöltése")

st.write("Letöltési paraméterek kiválasztása:")

broker_asset_matrix_df = load_broker_asset_matrix()

broker_options = get_brokers(
    broker_asset_matrix_df,
)

broker_mode = st.radio(
    "Bróker választás",
    options=[
        "Minden bróker",
        "Kiválasztott brókerek",
    ],
    horizontal=True,
)

if broker_mode == "Minden bróker":
    selected_brokers = None
    selected_brokers_for_ui = broker_options
else:
    broker_choices = st.multiselect(
        "Bróker",
        options=broker_options,
        default=[],
    )

    selected_brokers = broker_choices
    selected_brokers_for_ui = broker_choices

asset_options = get_assets(
    broker_asset_matrix_df=broker_asset_matrix_df,
    selected_brokers=selected_brokers_for_ui,
)

asset_mode = st.radio(
    "Ticker választás",
    options=[
        "Minden ticker",
        "Kiválasztott tickerek",
    ],
    horizontal=True,
)

if asset_mode == "Minden ticker":
    selected_assets = None
else:
    selected_assets = st.multiselect(
        "Ticker",
        options=asset_options,
        default=[],
    )

date_col_1, date_col_2 = st.columns(2)

with date_col_1:
    start_month = st.text_input(
        "Kezdő hónap",
        value=cfg.run.start_month or "2024-01",
        help="Formátum: YYYY-MM",
    )

with date_col_2:
    end_month = st.text_input(
        "Záró hónap",
        value=cfg.run.end_month or "2024-01",
        help="Formátum: YYYY-MM",
    )

st.divider()

if "download_check_completed" not in st.session_state:
    st.session_state["download_check_completed"] = False

if "download_check_error" not in st.session_state:
    st.session_state["download_check_error"] = ""

if "saxo_access_token" not in st.session_state:
    st.session_state["saxo_access_token"] = ""

if "tws_confirmed" not in st.session_state:
    st.session_state["tws_confirmed"] = False

if "download_results_df" not in st.session_state:
    st.session_state["download_results_df"] = None

check_requested = st.button(
    "Ellenőrzés",
    use_container_width=True,
)

if check_requested:
    try:
        validate_download_config(cfg)
        validate_month_range(
            start_month=start_month,
            end_month=end_month,
        )
        st.session_state["download_check_completed"] = True
        st.session_state["download_check_error"] = ""
    except Exception as error:
        st.session_state["download_check_completed"] = False
        st.session_state["download_check_error"] = str(error)

if st.session_state["download_check_error"]:
    st.error(st.session_state["download_check_error"])

if st.session_state["download_check_completed"]:
    st.success("Config validáció sikeres.")

    st.subheader("Futtatási előfeltételek")

    if selected_brokers is None or "saxo_bank" in selected_brokers:
        st.session_state["saxo_access_token"] = st.text_input(
            "Saxo access token",
            value=st.session_state["saxo_access_token"],
            type="password",
            help="Csak Saxo Bank futtatásakor szükséges.",
        )

    if selected_brokers is None or "interactive_brokers" in selected_brokers:
        st.warning(
            "Interactive Brokers futtatásához indítsd el a TWS-t, "
            "engedélyezd az API kapcsolatot, és ellenőrizd a 7497-es portot."
        )

        st.session_state["tws_confirmed"] = st.checkbox(
            "A TWS fut, és az API kapcsolat engedélyezve van.",
            value=st.session_state["tws_confirmed"],
        )

saxo_required = selected_brokers is None or "saxo_bank" in selected_brokers
tws_required = (
    selected_brokers is None
    or "interactive_brokers" in selected_brokers
)

saxo_ready = (
    not saxo_required
    or bool(st.session_state["saxo_access_token"].strip())
)

tws_ready = (
    not tws_required
    or st.session_state["tws_confirmed"]
)

start_disabled = (
    not st.session_state["download_check_completed"]
    or not saxo_ready
    or not tws_ready
)

start_requested = st.button(
    "Indítás",
    use_container_width=True,
    disabled=start_disabled,
)

if start_requested:
    ensure_event_loop()

    from pipelines.monthly_ingestion_runner import (
        run_monthly_ingestion_from_config,
    )

    with st.spinner("Letöltés folyamatban..."):
        results_df = run_monthly_ingestion_from_config(
            start_month=start_month,
            end_month=end_month,
            brokers=selected_brokers,
            assets=selected_assets,
            interval=cfg.run.interval,
            config_dir=PROJECT_ROOT / Path(cfg.paths.config_dir),
            env_path=PROJECT_ROOT / Path(cfg.paths.env_path),
            data_dir=PROJECT_ROOT / Path(cfg.paths.data_dir),
            saxo_access_token=(
                st.session_state["saxo_access_token"]
                if saxo_required
                else None
            ),
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

    st.session_state["download_results_df"] = results_df
    st.success("Letöltés befejeződött.")

if st.session_state["download_results_df"] is not None:
    st.subheader("Letöltési eredmény")
    st.dataframe(
        st.session_state["download_results_df"],
        use_container_width=True,
    )

st.divider()

st.subheader("Kiválasztott paraméterek")

st.write(
    {
        "brokers": selected_brokers,
        "assets": selected_assets,
        "start_month": start_month,
        "end_month": end_month,
        "saxo_token_provided": bool(st.session_state["saxo_access_token"]),
        "tws_confirmed": st.session_state["tws_confirmed"],
    }
)
