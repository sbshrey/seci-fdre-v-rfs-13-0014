"""Generate file-based output profile and aux power inputs from tender metadata."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.profile_templates import build_load_profile_frame


def generate_tender_input_files(config: ProjectConfig) -> list[Path]:
    timeline = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start=config.project.simulation_start,
                end=config.project.simulation_end,
                interval="1m",
                eager=True,
            )
        }
    )
    load_frame = build_load_profile_frame(timeline["timestamp"], config.simulation.load)
    output_df = timeline.with_columns(pl.Series("output_profile_kw", load_frame["output_profile_kw"]))
    aux_df = timeline.with_columns(pl.Series("aux_power_kw", load_frame["aux_consumption_kw"]))
    evening_df = output_df.with_columns(
        pl.when(pl.col("timestamp").dt.hour().is_between(18, 21, closed="both"))
        .then(pl.col("output_profile_kw"))
        .otherwise(0.0)
        .alias("output_profile_18_22_kw")
    ).select("timestamp", "output_profile_18_22_kw")

    _write_csv(output_df, config.inputs.output_profile_path)
    _write_csv(aux_df, config.inputs.aux_power_path)
    _write_csv(evening_df, config.inputs.output_profile_18_22_path)
    return [config.inputs.output_profile_path, config.inputs.output_profile_18_22_path, config.inputs.aux_power_path]


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path)
