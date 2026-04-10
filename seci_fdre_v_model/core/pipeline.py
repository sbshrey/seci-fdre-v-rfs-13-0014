"""Pipeline orchestration for section-based BESS simulations."""

from __future__ import annotations

import gc
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import polars as pl

from seci_fdre_v_model.config import SimulationConfig
from seci_fdre_v_model.data.loaders import load_consumption_data, load_generation_data
from seci_fdre_v_model.data.preprocessing import align_generation_to_minute
from seci_fdre_v_model.flows.section_outputs import section_accounting_stage, write_section_outputs
from seci_fdre_v_model.profile_templates import compute_profile_compliance_tables, compute_profile_summary_metrics
from seci_fdre_v_model.results import SimulationResult

StageFn = Callable[[pl.DataFrame, "SimulationContext"], pl.DataFrame]
ProgressCallback = Callable[[str, float, str], None]


@dataclass
class SimulationContext:
    """Shared state and helpers for section execution."""

    config: SimulationConfig
    logger: logging.Logger
    balance_tolerance_kw: float = 1e-3
    progress_callback: ProgressCallback | None = field(default=None, repr=False)

    def _progress(self, stage: str, pct: float, detail: str) -> None:
        if self.progress_callback:
            self.progress_callback(stage, pct, detail)

    def log_stage(self, stage_name: str, df: pl.DataFrame) -> None:
        """Emit concise stage-level logging."""
        self.logger.info("Completed stage %s with %s rows", stage_name, df.height)

    def validate_balance(self, df: pl.DataFrame) -> None:
        """Reject materially invalid identity equations."""
        max_abs_error = df.select(pl.col("identity_1_error_kw").abs().max()).item()
        if max_abs_error is None:
            return
        tolerance = _balance_tolerance_for_dtype(self.config.preprocessing.simulation_dtype)
        if max_abs_error > max(tolerance, self.balance_tolerance_kw):
            raise ValueError(f"Energy balance validation failed. Max error was {max_abs_error:.12f}.")


FLOW_STAGES: list[StageFn] = [section_accounting_stage]


def simulate_system(
    config: SimulationConfig,
    progress_callback: ProgressCallback | None = None,
) -> SimulationResult:
    """Run a single BESS simulation from source data to KPI summary."""
    logger = logging.getLogger(f"seci_fdre_v_model.{config.plant_name}")
    context = SimulationContext(config=config, logger=logger, progress_callback=progress_callback)

    context._progress("Loading data", 2, "Loading solar, wind, profile, and aux CSVs")
    solar, wind = load_generation_data(config)
    output_profile, aux_power = load_consumption_data(config)
    context._progress("Loading data", 8, f"Loaded {solar.height} solar, {wind.height} wind rows")

    context._progress("Aligning", 10, "Aligning to 1-minute grid")
    minute_data = align_generation_to_minute(solar, wind, config.preprocessing)
    minute_data = _align_consumption_to_minute(minute_data, output_profile, aux_power)
    context._progress("Aligning", 12, f"Aligned {minute_data.height} minutes")

    context._progress("Simulating", 15, f"Running section accounting ({minute_data.height} rows)")
    final_df = run_pipeline(minute_data, context, FLOW_STAGES)
    gc.collect()

    context._progress("Summary", 90, "Computing summary metrics")
    return build_simulation_result(final_df, config)


def load_aligned_inputs(config: SimulationConfig) -> tuple[pl.DataFrame, SimulationContext]:
    """Load raw generation inputs and return the aligned minute-level table."""
    logger = logging.getLogger(f"seci_fdre_v_model.{config.plant_name}")
    context = SimulationContext(config=config, logger=logger)
    solar, wind = load_generation_data(config)
    minute_data = align_generation_to_minute(solar, wind, config.preprocessing)
    output_profile, aux_power = load_consumption_data(config)
    minute_data = _align_consumption_to_minute(minute_data, output_profile, aux_power)
    return minute_data, context


def run_pipeline(
    df: pl.DataFrame,
    context: SimulationContext,
    stages: list[StageFn] | None = None,
) -> pl.DataFrame:
    """Apply each registered stage in sequence."""
    active_stages = stages or FLOW_STAGES
    result = df
    for stage in active_stages:
        result = stage(result, context)
        context.log_stage(stage.__name__, result)
    return result


def build_simulation_result(df: pl.DataFrame, config: SimulationConfig) -> SimulationResult:
    """Attach compliance tables and summary metrics to a completed minute-flow table."""
    block_df, monthly_df = compute_profile_compliance_tables(df, config.load)
    metrics = compute_summary_metrics(
        df,
        config,
        profile_compliance_blocks=block_df,
        profile_compliance_monthly=monthly_df,
    )
    return SimulationResult(
        minute_flows=df,
        summary_metrics=metrics,
        profile_compliance_blocks=block_df,
        profile_compliance_monthly=monthly_df,
    )


def compute_summary_metrics(
    df: pl.DataFrame,
    config: SimulationConfig,
    *,
    profile_compliance_blocks: pl.DataFrame | None = None,
    profile_compliance_monthly: pl.DataFrame | None = None,
) -> dict[str, float | int | str | None]:
    """Aggregate minute-level section outputs into KPI metrics (values in kW-min for consistency)."""
    grid_import = _sum_kw_min(df, "grid_buy_kw")
    total_consumption = _sum_kw_min(df, "total_consumption_kw")
    self_consumption_pct = (
        100.0 * (1.0 - grid_import / total_consumption) if total_consumption > 0 else 100.0
    )
    metrics: dict[str, float | int | str | None] = {
        "plant_name": config.plant_name,
        "rows": df.height,
        "grid_import_kw_min": grid_import,
        "grid_export_kw_min": _sum_kw_min(df, "grid_sell_kw"),
        "total_consumption_kw_min": total_consumption,
        "self_consumption_pct": self_consumption_pct,
        "final_degraded_capacity_kw_min": float(df.select(pl.col("capacity_now_kw_min").tail(1)).item()),
        "final_soc_pct": float(df.select(pl.col("soc_fraction").tail(1)).item()) * 100.0,
        "cumulative_drawn_kw_min": float(df.select(pl.col("battery_draw_cumulative_kw_min").tail(1)).item()),
        "cumulative_stored_kw_min": float(df.select(pl.col("battery_store_cumulative_kw_min").tail(1)).item()),
        "cumulative_charge_count": float(df.select(pl.col("cum_charge_count").tail(1)).item()),
        "identity_1_failures": int(df.select((1 - pl.col("identity_1_ok")).sum()).item()),
        "identity_2_failures": int(df.select((1 - pl.col("identity_2_ok")).sum()).item()),
        "max_identity_error_kw": float(df.select(pl.col("identity_1_error_kw").abs().max()).item()),
        "identity_2_max_error_kw_min": float(df.select(pl.col("identity_2_error_kw_min").abs().max()).item()),
    }
    metrics.update(
        compute_profile_summary_metrics(
            config.load,
            monthly_df=profile_compliance_monthly,
            block_df=profile_compliance_blocks,
        )
    )
    return metrics


def write_simulation_outputs(result: SimulationResult, output_dir: str | Path, stem: str) -> tuple[Path, Path]:
    """Persist minute-level flows, summary metrics, and energy table."""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = target_dir / f"{stem}_minute_flows.parquet"
    metrics_path = target_dir / f"{stem}_summary.csv"
    energy_table_path = target_dir / f"{stem}_energy_table.csv"
    compliance_blocks_path = target_dir / f"{stem}_profile_compliance_blocks.csv"
    compliance_monthly_path = target_dir / f"{stem}_profile_compliance_monthly.csv"
    result.minute_flows.write_parquet(parquet_path)
    pl.DataFrame([result.summary_metrics]).write_csv(metrics_path)
    energy_rows = compute_energy_table(result.minute_flows)
    pl.DataFrame(energy_rows).write_csv(energy_table_path)
    if result.profile_compliance_blocks is not None:
        result.profile_compliance_blocks.write_csv(compliance_blocks_path)
    elif compliance_blocks_path.exists():
        compliance_blocks_path.unlink()
    if result.profile_compliance_monthly is not None:
        result.profile_compliance_monthly.write_csv(compliance_monthly_path)
    elif compliance_monthly_path.exists():
        compliance_monthly_path.unlink()
    return parquet_path, metrics_path


def write_stage_outputs(
    df: pl.DataFrame,
    context: SimulationContext,
    output_dir: str | Path,
    stem: str,
    stages: list[StageFn] | None = None,
) -> list[Path]:
    """Write aligned input and section CSV outputs."""
    target_dir = Path(output_dir) / f"{stem}_sections"
    target_dir.mkdir(parents=True, exist_ok=True)

    written_paths: list[Path] = []
    input_path = target_dir / "00_aligned_input.csv"
    df.write_csv(input_path)
    written_paths.append(input_path)

    stage_df = run_pipeline(df, context, stages or FLOW_STAGES)
    written_paths.extend(write_section_outputs(stage_df, target_dir))
    return written_paths


def _sum_kw_min(df: pl.DataFrame, column: str) -> float:
    """Sum power (kW) over minutes to get energy in kW-min."""
    return float(df.select(pl.col(column).sum()).item())


def compute_energy_table(df: pl.DataFrame) -> list[dict[str, str | float]]:
    """Compute annual energy flows for SOURCES, USES, and LOSS (kW-min)."""
    rows: list[dict[str, str | float]] = []

    def add(category: str, element: str, value_kw_min: float) -> None:
        rows.append({"category": category, "element": element, "value_kw_min": value_kw_min})

    # SOURCES
    add("SOURCES", "Solar Power", _sum_kw_min(df, "solar_kw"))
    add("SOURCES", "Wind Power", _sum_kw_min(df, "wind_kw"))
    add("SOURCES", "Draw from BESS", _sum_kw_min(df, "battery_draw_final_kw"))
    add("SOURCES", "Draw from GRID", _sum_kw_min(df, "grid_buy_kw"))

    # USES
    add("USES", "Charge BESS", _sum_kw_min(df, "battery_store_final_kw"))
    add("USES", "Sell to GRID", _sum_kw_min(df, "grid_sell_kw"))
    add("USES", "Output (O/p)", _sum_kw_min(df, "total_consumption_kw"))

    # LOSS
    add("LOSS", "Discharge Loss", _sum_kw_min(df, "battery_draw_loss_kw"))
    add("LOSS", "Charge Loss", _sum_kw_min(df, "battery_store_loss_kw"))

    return rows


def _align_consumption_to_minute(minute_data: pl.DataFrame, output_profile: pl.DataFrame, aux_power: pl.DataFrame) -> pl.DataFrame:
    return (
        minute_data.join(output_profile, on="timestamp", how="left")
        .join(aux_power, on="timestamp", how="left")
        .with_columns(pl.col("output_profile_kw").fill_null(0.0), pl.col("aux_consumption_kw").fill_null(0.0))
        .with_columns((pl.col("output_profile_kw") + pl.col("aux_consumption_kw")).alias("total_consumption_kw"))
    )


def _balance_tolerance_for_dtype(dtype: str) -> float:
    return 1e-2 if dtype == "float32" else 1e-3
