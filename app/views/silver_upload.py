from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from hydra import compose, initialize_config_dir
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from config_loader.hydra_config import AppConfig
from pipelines.silver_generated_ohlcv_runner import (
    upload_silver_generated_ohlcv_from_dataframe,
)
from pipelines.silver_quality_runner import run_silver_quality_from_config
from uploaders.azure_blob import init_azure_client
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


def validate_silver_config(cfg: AppConfig) -> None:
    config_files = OmegaConf.to_container(
        cfg.config_files,
        resolve=True,
    )

    validate_config_files(
        config_dir=PROJECT_ROOT / Path(cfg.paths.config_dir),
        config_files=config_files,
        print_progress=False,
    )


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


def silver_success_blob_name(
    *,
    method_id: int,
    asset: str,
    year: int,
    month: int,
) -> str:
    return (
        f"silver/generated_ohlcv/method_{method_id}/"
        f"{asset.lower()}/{year}/{month:02d}/_SUCCESS"
    )


def build_silver_targets(
    *,
    methods: list[int],
    assets: list[str],
    start_month: str,
    end_month: str,
) -> pd.DataFrame:
    rows = []

    for method_id in methods:
        for asset in assets:
            for year, month in month_range(
                start_month=start_month,
                end_month=end_month,
            ):
                rows.append(
                    {
                        "method_id": method_id,
                        "asset": asset,
                        "year": year,
                        "month": month,
                        "month_key": f"{year}-{month:02d}",
                        "success_blob_name": silver_success_blob_name(
                            method_id=method_id,
                            asset=asset,
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
    overwrite: bool,
) -> pd.DataFrame:
    if overwrite:
        return targets_df.copy().reset_index(drop=True)

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
        "mar_feltoltve": int(targets_df["already_uploaded"].sum())
        if "already_uploaded" in targets_df.columns
        else 0,
        "futtatando": len(missing_targets_df),
    }


def build_check_key(
    *,
    selected_brokers: list[str] | None,
    assets_for_run: list[str],
    start_month: str,
    end_month: str,
    selected_methods: list[int],
    overwrite: bool,
) -> tuple:
    return (
        tuple(sorted(selected_brokers)) if selected_brokers is not None else None,
        tuple(sorted(assets_for_run)),
        start_month,
        end_month,
        tuple(sorted(selected_methods)),
        overwrite,
    )


def run_and_upload_one_target(
    *,
    method_id: int,
    asset: str,
    month_key: str,
    cfg: AppConfig,
    selected_brokers: list[str] | None,
    overwrite: bool,
) -> pd.DataFrame:
    result = run_silver_quality_from_config(
        start_month=month_key,
        end_month=month_key,
        brokers=selected_brokers,
        assets=[asset],
        interval=cfg.run.interval,
        generation_method=method_id,
        source=cfg.run.source,
        config_dir=PROJECT_ROOT / Path(cfg.paths.config_dir),
        env_path=PROJECT_ROOT / Path(cfg.paths.env_path),
        container_name=cfg.azure.container_name,
        data_dir=PROJECT_ROOT / Path(cfg.paths.data_dir),
        print_progress=cfg.run.print_progress,
        show_figures=False,
    )

    silver_ohlcv_df = result["silver_ohlcv"]

    if silver_ohlcv_df.empty:
        return pd.DataFrame(
            [
                {
                    "status": "skipped",
                    "asset": asset,
                    "year": int(month_key[:4]),
                    "month": int(month_key[5:7]),
                    "message": "No silver rows generated.",
                    "row_count": 0,
                    "method_id": method_id,
                    "generation_method": None,
                }
            ]
        )

    generation_method_name = (
        silver_ohlcv_df["generation_method"]
        .dropna()
        .iloc[0]
    )

    upload_results_df = upload_silver_generated_ohlcv_from_dataframe(
        silver_ohlcv_df=silver_ohlcv_df,
        interval=cfg.run.interval,
        generation_method_id=method_id,
        generation_method=generation_method_name,
        env_path=PROJECT_ROOT / Path(cfg.paths.env_path),
        data_dir=PROJECT_ROOT / Path(cfg.paths.data_dir),
        overwrite=overwrite,
        print_progress=cfg.run.print_progress,
    )

    upload_results_df["method_id"] = method_id
    upload_results_df["generation_method"] = generation_method_name

    return upload_results_df


cfg = load_hydra_config()

st.title("Silver feltöltés")

st.write("Silver generated OHLCV előállítása és feltöltése Azure-ba.")

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

assets_for_run = (
    asset_options
    if selected_assets is None
    else selected_assets
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

method_options = {
    "Method 0 - median OHLC": 0,
    "Method 1 - preferred broker OHLC": 1,
}

selected_method_labels = st.multiselect(
    "Generálási módszer",
    options=list(method_options.keys()),
    default=list(method_options.keys()),
)

selected_methods = [
    method_options[label]
    for label in selected_method_labels
]

overwrite = st.checkbox(
    "Meglévő silver fájlok felülírása",
    value=False,
)

current_check_key = build_check_key(
    selected_brokers=selected_brokers,
    assets_for_run=assets_for_run,
    start_month=start_month,
    end_month=end_month,
    selected_methods=selected_methods,
    overwrite=overwrite,
)

st.divider()

if "silver_check_completed" not in st.session_state:
    st.session_state["silver_check_completed"] = False

if "silver_check_error" not in st.session_state:
    st.session_state["silver_check_error"] = ""

if "silver_check_key" not in st.session_state:
    st.session_state["silver_check_key"] = None

if "silver_targets_df" not in st.session_state:
    st.session_state["silver_targets_df"] = None

if "silver_missing_targets_df" not in st.session_state:
    st.session_state["silver_missing_targets_df"] = None

if "silver_upload_results_df" not in st.session_state:
    st.session_state["silver_upload_results_df"] = None

check_requested = st.button(
    "Ellenőrzés",
    use_container_width=True,
)

if check_requested:
    try:
        validate_silver_config(cfg)

        if not selected_methods:
            raise ValueError("Legalább egy generálási módszert ki kell választani.")

        if not assets_for_run:
            raise ValueError("Legalább egy tickernek elérhetőnek kell lennie.")

        targets_df = build_silver_targets(
            methods=selected_methods,
            assets=assets_for_run,
            start_month=start_month,
            end_month=end_month,
        )

        targets_df = attach_existing_success_flags(
            targets_df=targets_df,
            env_path=PROJECT_ROOT / Path(cfg.paths.env_path),
            container_name=cfg.azure.container_name,
        )

        missing_targets_df = get_missing_targets(
            targets_df=targets_df,
            overwrite=overwrite,
        )

        st.session_state["silver_targets_df"] = targets_df
        st.session_state["silver_missing_targets_df"] = missing_targets_df
        st.session_state["silver_check_completed"] = True
        st.session_state["silver_check_error"] = ""
        st.session_state["silver_check_key"] = current_check_key
        st.session_state["silver_upload_results_df"] = None

    except Exception as error:
        st.session_state["silver_check_completed"] = False
        st.session_state["silver_check_error"] = str(error)
        st.session_state["silver_check_key"] = None
        st.session_state["silver_targets_df"] = None
        st.session_state["silver_missing_targets_df"] = None

if st.session_state["silver_check_error"]:
    st.error(st.session_state["silver_check_error"])

check_is_current = (
    st.session_state["silver_check_completed"]
    and st.session_state["silver_check_key"] == current_check_key
)

if st.session_state["silver_check_completed"]:
    if check_is_current:
        targets_df = st.session_state["silver_targets_df"]
        missing_targets_df = st.session_state["silver_missing_targets_df"]

        summary = summarize_targets(
            targets_df=targets_df,
            missing_targets_df=missing_targets_df,
        )

        st.success("Config és Azure _SUCCESS ellenőrzés sikeres.")

        st.write(
            {
                "összes cél": summary["ossz_cel"],
                "már feltöltve": summary["mar_feltoltve"],
                "futtatandó": summary["futtatando"],
            }
        )

        with st.expander("Futtatandó célok"):
            st.dataframe(
                missing_targets_df[
                    [
                        "method_id",
                        "asset",
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
    or st.session_state["silver_missing_targets_df"] is None
    or st.session_state["silver_missing_targets_df"].empty
)

start_requested = st.button(
    "Indítás",
    use_container_width=True,
    disabled=start_disabled,
)

if start_requested:
    missing_targets_df = st.session_state["silver_missing_targets_df"]

    all_upload_results = []

    progress_bar = st.progress(0)
    status_placeholder = st.empty()

    with st.spinner("Silver előállítás és feltöltés folyamatban..."):
        total_targets = len(missing_targets_df)

        for position, (_, target) in enumerate(
            missing_targets_df.iterrows(),
            start=1,
        ):
            method_id = int(target["method_id"])
            asset = str(target["asset"])
            month_key = str(target["month_key"])

            status_placeholder.write(
                f"Fut: method {method_id}, {asset}, {month_key}"
            )

            upload_results_df = run_and_upload_one_target(
                method_id=method_id,
                asset=asset,
                month_key=month_key,
                cfg=cfg,
                selected_brokers=selected_brokers,
                overwrite=overwrite,
            )

            all_upload_results.append(upload_results_df)

            progress_bar.progress(
                int(position / total_targets * 100)
            )

    if all_upload_results:
        st.session_state["silver_upload_results_df"] = pd.concat(
            all_upload_results,
            ignore_index=True,
        )
        st.success("Silver feltöltés befejeződött.")
    else:
        st.session_state["silver_upload_results_df"] = pd.DataFrame()
        st.warning("Nem történt silver feltöltés.")

if st.session_state["silver_upload_results_df"] is not None:
    st.subheader("Silver feltöltési eredmény")
    st.dataframe(
        st.session_state["silver_upload_results_df"],
        use_container_width=True,
    )

st.divider()

st.subheader("Kiválasztott paraméterek")

st.write(
    {
        "brokers": selected_brokers,
        "assets": selected_assets,
        "resolved_assets": assets_for_run,
        "start_month": start_month,
        "end_month": end_month,
        "generation_methods": selected_methods,
        "overwrite": overwrite,
        "check_is_current": check_is_current,
    }
)