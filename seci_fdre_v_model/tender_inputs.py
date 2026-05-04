"""Generate file-based output profile and aux power inputs from tender metadata."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from seci_fdre_v_model.config import FLAT_PROFILE_MODE, ProjectConfig
from seci_fdre_v_model.data.loaders import load_generation_data
from seci_fdre_v_model.data.preprocessing import align_generation_to_minute
from seci_fdre_v_model.profile_templates import build_load_profile_frame, expand_default_seci_shape_profile_kw


def generate_tender_input_files(config: ProjectConfig) -> list[Path]:
    load = config.simulation.load
    timeline = _project_timeline(config)

    written: list[Path] = []
    if load.uses_manual_profile:
        output_df = _read_manual_output_profile(config.inputs.output_profile_path)
    elif load.uses_template_profile:
        output_df = _build_normalized_seci_output_profile(config, timeline)
        _write_csv(output_df, config.inputs.output_profile_path, config.inputs.generated_decimal_places)
        written.append(config.inputs.output_profile_path)
    else:
        load_frame = build_load_profile_frame(
            timeline["timestamp"],
            load,
            battery_nominal_power_kw=config.simulation.battery.nominal_power_kw,
        )
        output_df = timeline.with_columns(pl.Series("output_profile_kw", load_frame["output_profile_kw"]))
        _write_csv(output_df, config.inputs.output_profile_path, config.inputs.generated_decimal_places)
        written.append(config.inputs.output_profile_path)

    evening_df = _build_evening_profile_frame(output_df, config)
    _write_csv(evening_df, config.inputs.output_profile_18_22_path, config.inputs.generated_decimal_places)
    written.append(config.inputs.output_profile_18_22_path)

    if load.uses_static_aux:
        written.append(_write_static_aux_power_file(config, timeline))
    return written


def generate_static_aux_power_file(config: ProjectConfig) -> Path | None:
    """Generate aux_power.csv for static_csv aux mode from load.aux_consumption_kw."""
    if not config.simulation.load.uses_static_aux:
        return None
    return _write_static_aux_power_file(config, _project_timeline(config))


def _project_timeline(config: ProjectConfig) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start=config.project.simulation_start,
                end=config.project.simulation_end,
                interval="1m",
                eager=True,
            )
        }
    )


def _write_static_aux_power_file(config: ProjectConfig, timeline: pl.DataFrame) -> Path:
    if config.inputs.aux_power_path is None:
        raise ValueError("inputs.aux_power_path is required in static_csv aux mode.")
    aux_df = timeline.with_columns(pl.lit(float(config.simulation.load.aux_consumption_kw)).alias("aux_power_kw"))
    _write_csv(aux_df, config.inputs.aux_power_path, config.inputs.generated_decimal_places)
    return config.inputs.aux_power_path


def _write_csv(frame: pl.DataFrame, path: Path, decimal_places: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path, float_precision=decimal_places)


def _build_normalized_seci_output_profile(config: ProjectConfig, timeline: pl.DataFrame) -> pl.DataFrame:
    base_profile_kw = expand_default_seci_shape_profile_kw(timeline["timestamp"])
    base_energy_kwh = float(base_profile_kw.sum()) / 60.0
    input_energy_kwh = _aligned_input_generation_energy_kwh(config, timeline)
    if base_energy_kwh <= 0:
        raise ValueError("SECI reference profile has no positive energy to normalize.")
    if input_energy_kwh <= 0:
        raise ValueError("Cannot normalize SECI output profile because active solar/wind input energy is zero.")
    scale = input_energy_kwh / base_energy_kwh
    return timeline.with_columns(pl.Series("output_profile_kw", base_profile_kw * scale))


def _aligned_input_generation_energy_kwh(config: ProjectConfig, timeline: pl.DataFrame) -> float:
    solar, wind = load_generation_data(config.simulation)
    aligned = align_generation_to_minute(solar, wind, config.simulation.preprocessing)
    scoped = (
        timeline.join(aligned.select("timestamp", "total_generation_kw"), on="timestamp", how="left")
        .with_columns(pl.col("total_generation_kw").fill_null(0.0).clip(lower_bound=0.0))
    )
    return float(scoped["total_generation_kw"].sum()) / 60.0


def _build_evening_profile_frame(output_df: pl.DataFrame, config: ProjectConfig) -> pl.DataFrame:
    profile_value = _evening_constant_profile_kw(config)
    if profile_value is None:
        evening_value = pl.col("output_profile_kw")
    else:
        evening_value = pl.lit(profile_value)
    return output_df.with_columns(
        pl.when(pl.col("timestamp").dt.hour().is_between(18, 21, closed="both"))
        .then(evening_value)
        .otherwise(0.0)
        .alias("output_profile_18_22_kw")
    ).select("timestamp", "output_profile_18_22_kw")


def _evening_constant_profile_kw(config: ProjectConfig) -> float | None:
    load = config.simulation.load
    if load.uses_manual_profile:
        return None
    if (load.profile_mode == FLAT_PROFILE_MODE or load.uses_time_based_profile) and load.output_profile_kw is not None:
        return float(load.output_profile_kw)
    if load.output_profile_18_22_kw is not None:
        return float(load.output_profile_18_22_kw)
    if load.output_profile_kw is not None:
        return float(load.output_profile_kw)
    if load.uses_template_profile:
        return float(load.contracted_capacity_mw or 0.0) * 1000.0
    return 0.0


def _read_manual_output_profile(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Manual profile mode requires an uploaded output profile CSV at {path}."
        )
    frame = pl.read_csv(path)
    missing_columns = {"timestamp", "output_profile_kw"}.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"output profile file is missing columns: {', '.join(sorted(missing_columns))}")
    normalized = (
        frame.select(
            pl.col("timestamp").cast(pl.String).str.strip_chars().alias("timestamp_raw"),
            pl.col("output_profile_kw").cast(pl.Float64).alias("output_profile_kw"),
        )
        .filter(pl.col("timestamp_raw") != "")
        .with_columns(
            pl.col("timestamp_raw")
            .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
            .alias("timestamp")
        )
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
        .select("timestamp", "output_profile_kw")
        .sort("timestamp")
    )
    if normalized.height == 0:
        raise ValueError("output profile file is empty after parsing.")
    null_count = normalized.select(
        pl.sum_horizontal(
            pl.col("timestamp").is_null().cast(pl.Int64),
            pl.col("output_profile_kw").is_null().cast(pl.Int64),
        ).sum()
    ).item()
    if null_count:
        raise ValueError("output profile file contains null timestamps or values.")
    duplicate_count = normalized.select(pl.col("timestamp").is_duplicated().sum()).item()
    if duplicate_count:
        raise ValueError("output profile file contains duplicate timestamps.")
    return normalized
