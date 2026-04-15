"""CSV loaders for generation and consumption profiles."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from seci_fdre_v_model.config import SimulationConfig

SOLAR_TIMESTAMP_COLUMN = "timestamp"
SOLAR_POWER_COLUMN = "Power in KW"
WIND_TIMESTAMP_COLUMN = "time stamp"
WIND_POWER_COLUMN = "Power in KW"
PROFILE_POWER_COLUMN = "output_profile_kw"
AUX_POWER_COLUMN = "aux_power_kw"


def load_generation_data(config: SimulationConfig) -> tuple[pl.DataFrame, pl.DataFrame]:
    load_solar = config.data.solar_enabled
    load_wind = config.data.wind_enabled
    if not load_solar and not load_wind:
        raise ValueError("At least one of solar_enabled or wind_enabled must be True.")

    solar = (
        _load_source_csv(
            path=config.data.solar_path,
            timestamp_column=SOLAR_TIMESTAMP_COLUMN,
            power_column=SOLAR_POWER_COLUMN,
            timestamp_format="%d/%m/%Y %H:%M",
            output_column="solar_kw",
            multiplier=config.data.solar_multiplier,
            source_name="solar",
        )
        if load_solar
        else None
    )
    wind = (
        _load_source_csv(
            path=config.data.wind_path,
            timestamp_column=WIND_TIMESTAMP_COLUMN,
            power_column=WIND_POWER_COLUMN,
            timestamp_format="%Y-%m-%d %H:%M",
            output_column="wind_kw",
            multiplier=config.data.wind_multiplier,
            source_name="wind",
        )
        if load_wind
        else None
    )

    if solar is not None and wind is not None:
        return solar, wind
    if solar is not None:
        return solar, solar.select("timestamp").with_columns(pl.lit(0.0).alias("wind_kw"))
    assert wind is not None
    return wind.select("timestamp").with_columns(pl.lit(0.0).alias("solar_kw")), wind


def load_consumption_data(config: SimulationConfig) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    output_profile = _load_profile_csv(
        path=config.load.output_profile_path or "",
        value_column=PROFILE_POWER_COLUMN,
        target_column="output_profile_kw",
        multiplier=config.load.profile_multiplier,
        source_name="output profile",
    )
    aux_profile = None
    if config.load.uses_static_aux:
        aux_profile = _load_profile_csv(
            path=config.load.aux_power_path or "",
            value_column=AUX_POWER_COLUMN,
            target_column="aux_consumption_kw",
            multiplier=1.0,
            source_name="aux power",
        )
    return output_profile, aux_profile


def _load_source_csv(
    *,
    path: str,
    timestamp_column: str,
    power_column: str,
    timestamp_format: str,
    output_column: str,
    multiplier: float,
    source_name: str,
) -> pl.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"{source_name} file not found: {csv_path}")
    frame = pl.read_csv(csv_path)
    missing_columns = {timestamp_column, power_column}.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"{source_name} file is missing columns: {', '.join(sorted(missing_columns))}")
    normalized = (
        frame.select(
            pl.col(timestamp_column).cast(pl.String).str.strip_chars().alias("timestamp_raw"),
            (pl.col(power_column).cast(pl.Float64) * float(multiplier)).alias(output_column),
        )
        .filter(pl.col("timestamp_raw") != "")
        .with_columns(pl.col("timestamp_raw").str.strptime(pl.Datetime, format=timestamp_format, strict=True).alias("timestamp"))
        .select("timestamp", output_column)
        .sort("timestamp")
    )
    _validate_frame(normalized, output_column, source_name)
    return normalized


def _load_profile_csv(
    *,
    path: str,
    value_column: str,
    target_column: str,
    multiplier: float,
    source_name: str,
) -> pl.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"{source_name} file not found: {csv_path}")
    frame = pl.read_csv(csv_path)
    missing_columns = {"timestamp", value_column}.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"{source_name} file is missing columns: {', '.join(sorted(missing_columns))}")
    normalized = (
        frame.select(
            pl.col("timestamp").cast(pl.String).str.strip_chars().alias("timestamp_raw"),
            (pl.col(value_column).cast(pl.Float64) * float(multiplier)).alias(target_column),
        )
        .filter(pl.col("timestamp_raw") != "")
        .with_columns(pl.col("timestamp_raw").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("timestamp"))
        .with_columns(
            pl.when(pl.col("timestamp").is_null())
            .then(pl.col("timestamp_raw").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False))
            .otherwise(pl.col("timestamp"))
            .alias("timestamp")
        )
        .with_columns(
            pl.when(pl.col("timestamp").is_null())
            .then(pl.col("timestamp_raw").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M", strict=False))
            .otherwise(pl.col("timestamp"))
            .alias("timestamp")
        )
        .select("timestamp", target_column)
        .sort("timestamp")
    )
    _validate_frame(normalized, target_column, source_name)
    return normalized


def _validate_frame(frame: pl.DataFrame, value_column: str, source_name: str) -> None:
    if frame.height == 0:
        raise ValueError(f"{source_name} dataset is empty after parsing.")
    null_count = frame.select(
        pl.sum_horizontal(
            pl.col("timestamp").is_null().cast(pl.Int64),
            pl.col(value_column).is_null().cast(pl.Int64),
        ).sum()
    ).item()
    if null_count:
        raise ValueError(f"{source_name} dataset contains null timestamps or values.")
    duplicate_count = frame.select(pl.col("timestamp").is_duplicated().sum()).item()
    if duplicate_count:
        raise ValueError(f"{source_name} dataset contains duplicate timestamps.")
