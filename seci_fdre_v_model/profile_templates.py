"""Tender-driven output profile templates and compliance helpers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path

import numpy as np
import polars as pl

from seci_fdre_v_model.config import (
    LoadConfig,
    SECI_REVISED_ANNEXURE_B_TEMPLATE_ID,
    TIME_BASED_PROFILE_DURATIONS,
)

MONTH_COLUMNS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
MONTH_INDEX_BY_NAME = {name: index for index, name in enumerate(MONTH_COLUMNS, start=1)}


@dataclass(frozen=True)
class TenderProfileTemplate:
    """Static metadata for a supported tender profile."""

    template_id: str
    asset_file: str
    base_capacity_mw: float
    block_minutes: int
    required_dfr: float
    source_doc: str


SUPPORTED_TENDER_PROFILES: dict[str, TenderProfileTemplate] = {
    SECI_REVISED_ANNEXURE_B_TEMPLATE_ID: TenderProfileTemplate(
        template_id=SECI_REVISED_ANNEXURE_B_TEMPLATE_ID,
        asset_file="seci_fdre_ii_revised_annexure_b.csv",
        base_capacity_mw=1500.0,
        block_minutes=15,
        required_dfr=0.90,
        source_doc="docs/SECI000116-2493087-RevisedAnnexure-BFDRE-II.pdf",
    ),
}


def get_tender_profile(template_id: str) -> TenderProfileTemplate:
    """Return metadata for a supported tender profile."""
    try:
        return SUPPORTED_TENDER_PROFILES[template_id]
    except KeyError as exc:
        supported = ", ".join(sorted(SUPPORTED_TENDER_PROFILES))
        raise ValueError(f"Unsupported tender profile '{template_id}'. Expected one of: {supported}.") from exc


def build_load_profile_frame(
    timestamps: pl.Series,
    load_config: LoadConfig,
    *,
    battery_nominal_power_kw: float | None = None,
) -> pl.DataFrame:
    """Build minute-level output/aux/total consumption columns for generated profile modes."""
    if timestamps.dtype != pl.Datetime:
        timestamps = timestamps.cast(pl.Datetime)

    if load_config.uses_template_profile:
        output_profile = _expand_template_output_profile(timestamps, load_config)
    elif load_config.uses_manual_profile:
        raise ValueError("Manual profile mode requires an uploaded output_profile.csv file.")
    elif load_config.uses_time_based_profile:
        output_profile = _expand_time_based_output_profile(
            timestamps,
            output_kw=float(load_config.output_profile_kw or 0.0),
            duration_hours=TIME_BASED_PROFILE_DURATIONS[load_config.profile_mode],
        )
    else:
        output_kw = float(load_config.output_profile_kw or 0.0)
        output_profile = np.full(len(timestamps), output_kw, dtype=np.float64)

    if load_config.uses_battery_state_aux:
        if battery_nominal_power_kw is None:
            raise ValueError("battery_nominal_power_kw is required for battery_state aux mode.")
        aux_kw = float(battery_nominal_power_kw) * float(load_config.aux_idle_fraction or 0.0)
    else:
        aux_kw = float(load_config.aux_consumption_kw)

    aux_consumption = np.full(len(timestamps), aux_kw, dtype=np.float64)
    total_consumption = output_profile + aux_consumption
    return pl.DataFrame(
        {
            "output_profile_kw": output_profile,
            "aux_consumption_kw": aux_consumption,
            "total_consumption_kw": total_consumption,
        }
    )


def compute_profile_compliance_tables(
    df: pl.DataFrame,
    load_config: LoadConfig,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """Compute block-level and monthly tender compliance tables."""
    if not load_config.uses_template_profile:
        return None, None

    template = get_tender_profile(load_config.profile_template_id or "")
    block_granularity = f"{template.block_minutes}m"
    required_dfr = template.required_dfr

    working = df.select(
        pl.col("timestamp").cast(pl.Datetime),
        pl.col("output_profile_kw").cast(pl.Float64),
        pl.col("total_consumption_kw").cast(pl.Float64),
        pl.col("grid_buy_kw").cast(pl.Float64),
    ).with_columns(
        month_index=pl.col("timestamp").dt.month(),
        month=pl.col("timestamp").dt.strftime("%b"),
        block_start=pl.col("timestamp").dt.truncate(block_granularity),
        project_supply_total_kw=(pl.col("total_consumption_kw") - pl.col("grid_buy_kw")).clip(lower_bound=0.0),
    ).with_columns(
        project_supply_to_profile_kw=pl.min_horizontal(
            "output_profile_kw",
            "project_supply_total_kw",
        ),
    )

    block_df = (
        working.group_by("block_start", "month_index", "month")
        .agg(
            minutes_in_block=pl.len(),
            block_target_kw=pl.col("output_profile_kw").mean(),
            block_target_kwh=pl.col("output_profile_kw").sum() / 60.0,
            block_supplied_kwh=pl.col("project_supply_to_profile_kw").sum() / 60.0,
        )
        .sort("block_start")
        .with_columns(
            block_dfr=pl.when(pl.col("block_target_kwh") > 0)
            .then((pl.col("block_supplied_kwh") / pl.col("block_target_kwh")).clip(upper_bound=1.0))
            .otherwise(1.0),
            required_dfr=pl.lit(required_dfr),
            required_dfr_pct=pl.lit(required_dfr * 100.0),
        )
        .with_columns(block_dfr_pct=pl.col("block_dfr") * 100.0)
    )

    monthly_df = (
        block_df.group_by("month_index", "month")
        .agg(
            blocks_in_month=pl.len(),
            profile_target_kwh=pl.col("block_target_kwh").sum(),
            profile_supplied_kwh=pl.col("block_supplied_kwh").sum(),
            monthly_dfr=pl.col("block_dfr").mean(),
        )
        .sort("month_index")
        .with_columns(
            monthly_dfr_pct=pl.col("monthly_dfr") * 100.0,
            required_dfr=pl.lit(required_dfr),
            required_dfr_pct=pl.lit(required_dfr * 100.0),
            dfr_ok=pl.col("monthly_dfr") >= required_dfr,
        )
    )

    return block_df, monthly_df


def compute_profile_summary_metrics(
    load_config: LoadConfig,
    monthly_df: pl.DataFrame | None,
    block_df: pl.DataFrame | None,
) -> dict[str, float | int | str | None]:
    """Build summary metrics related to tender profile compliance."""
    if not load_config.uses_template_profile:
        return {
            "profile_template_id": load_config.profile_mode,
            "required_dfr_pct": None,
            "min_monthly_dfr_pct": None,
            "months_below_dfr_threshold": None,
            "annual_energy_target_kwh": None,
            "annual_profile_target_kwh": None,
            "annual_profile_supplied_kwh": None,
            "annual_energy_gap_kwh": None,
        }

    template = get_tender_profile(load_config.profile_template_id or "")
    annual_energy_target_kwh = 0.0
    annual_profile_target_kwh = 0.0
    annual_profile_supplied_kwh = 0.0
    min_monthly_dfr_pct: float | None = None
    months_below_dfr_threshold = 0

    if block_df is not None and block_df.height > 0:
        annual_profile_target_kwh = float(block_df["block_target_kwh"].sum())
        annual_profile_supplied_kwh = float(block_df["block_supplied_kwh"].sum())
        annual_energy_target_kwh = annual_profile_target_kwh

    if monthly_df is not None and monthly_df.height > 0:
        min_monthly_dfr_pct = float(monthly_df["monthly_dfr_pct"].min())
        months_below_dfr_threshold = int((~monthly_df["dfr_ok"]).sum())

    return {
        "profile_template_id": template.template_id,
        "required_dfr_pct": template.required_dfr * 100.0,
        "min_monthly_dfr_pct": min_monthly_dfr_pct,
        "months_below_dfr_threshold": months_below_dfr_threshold,
        "annual_energy_target_kwh": annual_energy_target_kwh,
        "annual_profile_target_kwh": annual_profile_target_kwh,
        "annual_profile_supplied_kwh": annual_profile_supplied_kwh,
        "annual_energy_gap_kwh": annual_energy_target_kwh - annual_profile_supplied_kwh,
    }


@lru_cache(maxsize=None)
def _load_template_blocks(template_id: str) -> pl.DataFrame:
    template = get_tender_profile(template_id)
    asset_path = resources.files("seci_fdre_v_model").joinpath("profile_assets", template.asset_file)
    with resources.as_file(asset_path) as resolved_path:
        raw = pl.read_csv(Path(resolved_path))

    expected_columns = {"start_minute_of_day", *MONTH_COLUMNS}
    missing = expected_columns.difference(raw.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"Profile asset for '{template_id}' is missing required columns: {missing_cols}.")

    return (
        raw.unpivot(
            on=list(MONTH_COLUMNS),
            index="start_minute_of_day",
            variable_name="month_name",
            value_name="profile_mw",
        )
        .with_columns(
            month_index=pl.col("month_name").replace_strict(MONTH_INDEX_BY_NAME, return_dtype=pl.Int8),
            profile_mw=pl.col("profile_mw").cast(pl.Float64),
            start_minute_of_day=pl.col("start_minute_of_day").cast(pl.Int32),
        )
        .select("start_minute_of_day", "month_index", "profile_mw")
        .sort("month_index", "start_minute_of_day")
    )


def _expand_template_output_profile(timestamps: pl.Series, load_config: LoadConfig) -> np.ndarray:
    template = get_tender_profile(load_config.profile_template_id or "")
    scale = float(load_config.contracted_capacity_mw or 0.0) / template.base_capacity_mw
    return _expand_template_shape_profile_kw(timestamps, template.template_id, scale=scale)


def expand_default_seci_shape_profile_kw(timestamps: pl.Series) -> np.ndarray:
    return _expand_template_shape_profile_kw(timestamps, SECI_REVISED_ANNEXURE_B_TEMPLATE_ID, scale=1.0)


def _expand_template_shape_profile_kw(timestamps: pl.Series, template_id: str, *, scale: float = 1.0) -> np.ndarray:
    template = get_tender_profile(template_id)
    blocks = _load_template_blocks(template.template_id)
    expanded = (
        pl.DataFrame({"timestamp": timestamps})
        .with_columns(
            month_index=pl.col("timestamp").dt.month().cast(pl.Int8),
            minute_of_day=(
                (pl.col("timestamp").dt.hour().cast(pl.Int32) * 60)
                + pl.col("timestamp").dt.minute().cast(pl.Int32)
            ).cast(pl.Int32),
        )
        .with_columns(
            block_start_minute=(
                (pl.col("minute_of_day") // template.block_minutes) * template.block_minutes
            ).cast(pl.Int32)
        )
        .join(
            blocks,
            left_on=["block_start_minute", "month_index"],
            right_on=["start_minute_of_day", "month_index"],
            how="left",
        )
        .with_columns(output_profile_kw=pl.col("profile_mw") * scale * 1000.0)
    )

    if expanded["output_profile_kw"].null_count() > 0:
        raise ValueError(
            f"Unable to expand tender profile '{template.template_id}' for all simulation timestamps."
        )
    return expanded["output_profile_kw"].to_numpy()


def _expand_time_based_output_profile(
    timestamps: pl.Series,
    *,
    output_kw: float,
    duration_hours: int,
    start_hour: int = 18,
) -> np.ndarray:
    start_minute = start_hour * 60
    end_minute = start_minute + (duration_hours * 60)
    expanded = (
        pl.DataFrame({"timestamp": timestamps})
        .with_columns(
            minute_of_day=(
                (pl.col("timestamp").dt.hour().cast(pl.Int32) * 60)
                + pl.col("timestamp").dt.minute().cast(pl.Int32)
            ).cast(pl.Int32)
        )
        .with_columns(
            output_profile_kw=pl.when(
                pl.col("minute_of_day").is_between(start_minute, end_minute - 1, closed="both")
            )
            .then(pl.lit(float(output_kw)))
            .otherwise(0.0)
        )
    )
    return expanded["output_profile_kw"].to_numpy()
