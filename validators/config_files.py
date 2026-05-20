from __future__ import annotations

from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS: dict[str, set[str]] = {
    "broker_strategy": {
        "broker",
        "window_type",
        "enabled",
        "rate_limit_sleep_sec",
        "notes",
    },
    "broker_asset_matrix": {
        "broker",
    },
    "broker_asset_settings": {
        "broker",
        "asset",
        "price_scale",
    },
    "download_period": {
        "start_date",
        "end_date",
    },
    "saxo_bank_instruments": {
        "asset",
        "symbol",
        "uic",
        "asset_type",
        "volume_policy",
    },
    "interactive_brokers_instruments": {
        "asset",
        "symbol",
        "con_id",
        "security_type",
        "exchange",
        "currency",
        "what_to_show",
        "volume_policy",
        "notes",
    },
    "interactive_brokers_calendar_instruments": {
        "asset",
        "symbol",
        "con_id",
        "security_type",
        "exchange",
        "currency",
        "calendar_source",
        "calendar_timezone",
        "notes",
    },
    "silver_quality_thresholds": {
        "asset",
        "good_close_diff_pct_threshold",
        "warning_close_diff_pct_threshold",
        "outlier_abs_return_pct_threshold",
    },
}


def validate_config_files(
    *,
    config_dir: Path,
    config_files: dict[str, str],
    print_progress: bool = True,
) -> None:
    missing_files = []

    for config_name, file_name in config_files.items():
        file_path = config_dir / str(file_name)

        if file_path.exists():
            if print_progress:
                print(f"OK      {config_name}: {file_path}")

            validate_required_columns(
                config_name=str(config_name),
                file_path=file_path,
            )
            validate_csv_content(
                config_name=str(config_name),
                file_path=file_path,
            )
        else:
            if print_progress:
                print(f"MISSING {config_name}: {file_path}")

            missing_files.append(file_path)

    if missing_files:
        missing_text = "\n".join(
            str(path)
            for path in missing_files
        )
        raise FileNotFoundError(
            f"Missing config files:\n{missing_text}"
        )


def validate_required_columns(
    *,
    config_name: str,
    file_path: Path,
) -> None:
    required_columns = REQUIRED_COLUMNS.get(config_name)

    if required_columns is None:
        return

    df = pd.read_csv(
        file_path,
        nrows=0,
    )

    existing_columns = set(df.columns)
    missing_columns = required_columns - existing_columns

    if missing_columns:
        missing_text = ", ".join(
            sorted(missing_columns)
        )
        raise ValueError(
            f"{file_path} missing required columns: {missing_text}"
        )


def validate_csv_content(
    *,
    config_name: str,
    file_path: Path,
) -> None:
    if config_name == "download_period":
        validate_download_period(
            file_path=file_path,
        )
    elif config_name == "silver_quality_thresholds":
        validate_silver_quality_thresholds(
            file_path=file_path,
        )


def validate_download_period(
    *,
    file_path: Path,
) -> None:
    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError(f"{file_path} must contain at least one row")

    parsed_start = pd.to_datetime(
        df["start_date"],
        errors="coerce",
    )
    parsed_end = pd.to_datetime(
        df["end_date"],
        errors="coerce",
    )

    if parsed_start.isna().any():
        raise ValueError(f"{file_path} contains invalid start_date values")

    if parsed_end.isna().any():
        raise ValueError(f"{file_path} contains invalid end_date values")

    invalid_order = parsed_start > parsed_end

    if invalid_order.any():
        raise ValueError(f"{file_path} contains start_date after end_date")


def validate_silver_quality_thresholds(
    *,
    file_path: Path,
) -> None:
    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError(f"{file_path} must contain at least one row")

    if "DEFAULT" not in set(df["asset"].astype(str)):
        raise ValueError(f"{file_path} must contain a DEFAULT row")

    numeric_columns = [
        "good_close_diff_pct_threshold",
        "warning_close_diff_pct_threshold",
        "outlier_abs_return_pct_threshold",
    ]

    for column in numeric_columns:
        numeric_values = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        if numeric_values.isna().any():
            raise ValueError(f"{file_path} contains non-numeric {column}")

        if (numeric_values < 0).any():
            raise ValueError(f"{file_path} contains negative {column}")

    good_threshold = pd.to_numeric(
        df["good_close_diff_pct_threshold"],
        errors="coerce",
    )
    warning_threshold = pd.to_numeric(
        df["warning_close_diff_pct_threshold"],
        errors="coerce",
    )

    if (good_threshold > warning_threshold).any():
        raise ValueError(
            f"{file_path} contains good threshold greater than warning threshold"
        )
