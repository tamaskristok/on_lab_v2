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
from uploaders.azure_blob import init_azure_client
from validators.config_files import validate_config_files


try:
    asyncio.get_event_loop_policy().get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(
        asyncio.new_event_loop()
    )


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
HYDRA_CONFIG_DIR = PROJECT_ROOT / "conf"
BROKER_ASSET_MATRIX_PATH = CONFIG_DIR / "broker_asset_matrix.csv"
BROKER_STRATEGY_PATH = CONFIG_DIR / "broker_strategy.csv"


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


@st.cache_data
def load_broker_strategy() -> pd.DataFrame:
    return pd.read_csv(
        BROKER_STRATEGY_PATH,
        comment="#",
    )


def ensure_event_loop() -> None:
    try:
        asyncio.get_event_loop_policy().get_event_loop()
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


def get_enabled_brokers(
    broker_strategy_df: pd.DataFrame,
) -> list[str]:
    enabled_df = broker_strategy_df[
        broker_strategy_df["enabled"] == True
    ]

    return (
        enabled_df["broker"]
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


def month_range(
    *,
    start_month: str,
    end_month: str,
) -> list[tuple[int, int]]:
    periods = pd.period_range(
        start=start_month,
        end=end_month,
        freq="M",
    )

    return [
        (
            period.year,
            period.month,
        )
        for period in periods
    ]


def bronze_success_blob_name(
    *,
    broker: str,
    asset: str,
    year: int,
    month: int,
) -> str:
    return (
        f"bronze/{broker}/{asset.lower()}/{year}/{month:02d}/_SUCCESS"
    )


def broker_asset_pairs(
    *,
    broker_asset_matrix_df: pd.DataFrame,
    broker_options: list[str],
    selected_brokers: list[str] | None,
    selected_assets: list[str] | None,
) -> list[dict[str, str]]:
    brokers_for_run = (
        broker_options
        if selected_brokers is None
        else selected_brokers
    )

    asset_columns = [
        column
        for column in broker_asset_matrix_df.columns
        if column != "broker"
    ]

    assets_for_run = (
        asset_columns
        if selected_assets is None
        else selected_assets
    )

    rows = []

    for _, broker_row in broker_asset_matrix_df.iterrows():
        broker = str(broker_row["broker"])

        if broker not in brokers_for_run:
            continue

        for asset in assets_for_run:
            if asset not in broker_row.index:
                continue

            broker_symbol = broker_row[asset]

            if pd.isna(broker_symbol):
                continue

            broker_symbol = str(broker_symbol).strip()

            if broker_symbol == "" or broker_symbol == "NOK":
                continue

            rows.append(
                {
                    "broker": broker,
                    "asset": asset,
                    "broker_symbol": broker_symbol,
                }
            )

    return rows


def build_download_targets(
    *,
    broker_asset_matrix_df: pd.DataFrame,
    broker_options: list[str],
    selected_brokers: list[str] | None,
    selected_assets: list[str] | None,
    start_month: str,
    end_month: str,
) -> pd.DataFrame:
    rows = []

    pairs = broker_asset_pairs(
        broker_asset_matrix_df=broker_asset_matrix_df,
        broker_options=broker_options,
        selected_brokers=selected_brokers,
        selected_assets=selected_assets,
    )

    for pair in pairs:
        for year, month in month_range(
            start_month=start_month,
            end_month=end_month,
        ):
            rows.append(
                {
                    "broker": pair["broker"],
                    "asset": pair["asset"],
                    "broker_symbol": pair["broker_symbol"],
                    "year": year,
                    "month": month,
                    "month_key": f"{year}-{month:02d}",
                    "success_blob_name": bronze_success_blob_name(
                        broker=pair["broker"],
                        asset=pair["asset"],
                        year=year,
                        month=month,
                    ),
                }
            )

    return pd.DataFrame(rows)


def attach_existing_success_flags(
    *,
    targets_df: pd.DataFrame,
    env_path: Path,
    container_name: str,
) -> pd.DataFrame:
    if targets_df.empty:
        checked_df = targets_df.copy()
        checked_df["already_uploaded"] = pd.Series(dtype=bool)
        return checked_df

    blob_service_client, resolved_container_name = init_azure_client(
        env_path=env_path,
        container_name=container_name,
    )
    container_client = blob_service_client.get_container_client(
        resolved_container_name,
    )

    checked_df = targets_df.copy()

    checked_df["already_uploaded"] = checked_df["success_blob_name"].apply(
        lambda blob_name: container_client.get_blob_client(blob_name).exists()
    )

    return checked_df


def get_missing_targets(
    *,
    targets_df: pd.DataFrame,
) -> pd.DataFrame:
    return (
        targets_df[~targets_df["already_uploaded"]]
        .copy()
        .reset_index(drop=True)
    )


def summarize_targets(
    *,
    targets_df: pd.DataFrame,
    missing_targets_df: pd.DataFrame,
) -> dict[str, int]:
    return {
        "ossz_cel": len(targets_df),
        "mar_letoltve": int(targets_df["already_uploaded"].sum())
        if "already_uploaded" in targets_df.columns
        else 0,
        "futtatando": len(missing_targets_df),
    }


def build_check_key(
    *,
    selected_brokers: list[str] | None,
    selected_assets: list[str] | None,
    start_month: str,
    end_month: str,
    saxo_access_token: str,
    tws_confirmed: bool,
) -> tuple:
    return (
        tuple(sorted(selected_brokers)) if selected_brokers is not None else None,
        tuple(sorted(selected_assets)) if selected_assets is not None else None,
        start_month,
        end_month,
        bool(saxo_access_token.strip()),
        tws_confirmed,
    )


def run_one_download_target(
    *,
    target: pd.Series,
    cfg: AppConfig,
    saxo_access_token: str,
    run_monthly_ingestion_from_config,
) -> pd.DataFrame:
    broker = str(target["broker"])
    asset = str(target["asset"])
    month_key = str(target["month_key"])

    return run_monthly_ingestion_from_config(
        start_month=month_key,
        end_month=month_key,
        brokers=[broker],
        assets=[asset],
        interval=cfg.run.interval,
        config_dir=PROJECT_ROOT / Path(cfg.paths.config_dir),
        env_path=PROJECT_ROOT / Path(cfg.paths.env_path),
        data_dir=PROJECT_ROOT / Path(cfg.paths.data_dir),
        saxo_access_token=(
            saxo_access_token
            if broker == "saxo_bank"
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


cfg = load_hydra_config()

st.title("Bróker adatok letöltése")

st.write("Letöltési paraméterek kiválasztása:")

broker_asset_matrix_df = load_broker_asset_matrix()
broker_strategy_df = load_broker_strategy()

broker_options = get_enabled_brokers(
    broker_strategy_df,
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
    selected_brokers = st.multiselect(
        "Bróker",
        options=broker_options,
        default=[],
    )
    selected_brokers_for_ui = selected_brokers

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

if "download_check_key" not in st.session_state:
    st.session_state["download_check_key"] = None

if "download_targets_df" not in st.session_state:
    st.session_state["download_targets_df"] = None

if "download_missing_targets_df" not in st.session_state:
    st.session_state["download_missing_targets_df"] = None

if "saxo_access_token" not in st.session_state:
    st.session_state["saxo_access_token"] = ""

if "tws_confirmed" not in st.session_state:
    st.session_state["tws_confirmed"] = False

if "download_results_df" not in st.session_state:
    st.session_state["download_results_df"] = None

saxo_required = selected_brokers is None or "saxo_bank" in selected_brokers
tws_required = (
    selected_brokers is None
    or "interactive_brokers" in selected_brokers
)

st.subheader("Futtatási előfeltételek")

if saxo_required:
    st.session_state["saxo_access_token"] = st.text_input(
        "Saxo access token",
        value=st.session_state["saxo_access_token"],
        type="password",
        help="Csak Saxo Bank futtatásakor szükséges.",
    )

if tws_required:
    st.warning(
        "Interactive Brokers futtatásához indítsd el a TWS-t, "
        "engedélyezd az API kapcsolatot, és ellenőrizd a 7497-es portot."
    )

    st.session_state["tws_confirmed"] = st.checkbox(
        "A TWS fut, és az API kapcsolat engedélyezve van.",
        value=st.session_state["tws_confirmed"],
    )

saxo_ready = (
    not saxo_required
    or bool(st.session_state["saxo_access_token"].strip())
)

tws_ready = (
    not tws_required
    or st.session_state["tws_confirmed"]
)

current_check_key = build_check_key(
    selected_brokers=selected_brokers,
    selected_assets=selected_assets,
    start_month=start_month,
    end_month=end_month,
    saxo_access_token=st.session_state["saxo_access_token"],
    tws_confirmed=st.session_state["tws_confirmed"],
)

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

        if selected_brokers is not None and not selected_brokers:
            raise ValueError("Legalább egy brókert ki kell választani.")

        if selected_assets is not None and not selected_assets:
            raise ValueError("Legalább egy tickert ki kell választani.")

        if not saxo_ready:
            raise ValueError("Saxo Bank futtatásához access token szükséges.")

        if not tws_ready:
            raise ValueError("Interactive Brokers futtatásához TWS megerősítés szükséges.")

        targets_df = build_download_targets(
            broker_asset_matrix_df=broker_asset_matrix_df,
            broker_options=broker_options,
            selected_brokers=selected_brokers,
            selected_assets=selected_assets,
            start_month=start_month,
            end_month=end_month,
        )

        if targets_df.empty:
            raise ValueError("Nincs futtatható broker-ticker-hónap kombináció.")

        targets_df = attach_existing_success_flags(
            targets_df=targets_df,
            env_path=PROJECT_ROOT / Path(cfg.paths.env_path),
            container_name=cfg.azure.container_name,
        )

        missing_targets_df = get_missing_targets(
            targets_df=targets_df,
        )

        st.session_state["download_targets_df"] = targets_df
        st.session_state["download_missing_targets_df"] = missing_targets_df
        st.session_state["download_check_completed"] = True
        st.session_state["download_check_error"] = ""
        st.session_state["download_check_key"] = current_check_key
        st.session_state["download_results_df"] = None

    except Exception as error:
        st.session_state["download_check_completed"] = False
        st.session_state["download_check_error"] = str(error)
        st.session_state["download_check_key"] = None
        st.session_state["download_targets_df"] = None
        st.session_state["download_missing_targets_df"] = None

if st.session_state["download_check_error"]:
    st.error(st.session_state["download_check_error"])

check_is_current = (
    st.session_state["download_check_completed"]
    and st.session_state["download_check_key"] == current_check_key
)

if st.session_state["download_check_completed"]:
    if check_is_current:
        targets_df = st.session_state["download_targets_df"]
        missing_targets_df = st.session_state["download_missing_targets_df"]

        summary = summarize_targets(
            targets_df=targets_df,
            missing_targets_df=missing_targets_df,
        )

        st.success("Config, előfeltétel és Azure _SUCCESS ellenőrzés sikeres.")

        st.write(
            {
                "összes cél": summary["ossz_cel"],
                "már letöltve": summary["mar_letoltve"],
                "futtatandó": summary["futtatando"],
            }
        )

        with st.expander("Futtatandó célok"):
            st.dataframe(
                missing_targets_df[
                    [
                        "broker",
                        "asset",
                        "broker_symbol",
                        "month_key",
                        "already_uploaded",
                    ]
                ],
                use_container_width=True,
            )
    else:
        st.warning(
            "A paraméterek változtak. Indítás előtt futtasd újra az ellenőrzést."
        )

start_disabled = (
    not check_is_current
    or st.session_state["download_missing_targets_df"] is None
    or st.session_state["download_missing_targets_df"].empty
)

start_requested = st.button(
    "Indítás",
    use_container_width=True,
    disabled=start_disabled,
)

if start_requested:
    ensure_event_loop()

    from pipelines.monthly_ingestion_runner import run_monthly_ingestion_from_config

    missing_targets_df = st.session_state["download_missing_targets_df"]

    all_results = []

    progress_bar = st.progress(0)
    status_placeholder = st.empty()

    with st.spinner("Letöltés folyamatban..."):
        total_targets = len(missing_targets_df)

        for position, (_, target) in enumerate(
            missing_targets_df.iterrows(),
            start=1,
        ):
            broker = str(target["broker"])
            asset = str(target["asset"])
            month_key = str(target["month_key"])

            status_placeholder.write(
                f"Fut: {broker}, {asset}, {month_key}"
            )

            results_df = run_one_download_target(
                target=target,
                cfg=cfg,
                saxo_access_token=st.session_state["saxo_access_token"],
                run_monthly_ingestion_from_config=run_monthly_ingestion_from_config,
            )

            all_results.append(results_df)

            progress_bar.progress(
                int(position / total_targets * 100)
            )

    if all_results:
        st.session_state["download_results_df"] = pd.concat(
            all_results,
            ignore_index=True,
        )
        st.success("Letöltés befejeződött.")
    else:
        st.session_state["download_results_df"] = pd.DataFrame()
        st.warning("Nem történt letöltés.")

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
        "saxo_required": saxo_required,
        "saxo_token_provided": bool(st.session_state["saxo_access_token"]),
        "tws_required": tws_required,
        "tws_confirmed": st.session_state["tws_confirmed"],
        "check_is_current": check_is_current,
    }
)