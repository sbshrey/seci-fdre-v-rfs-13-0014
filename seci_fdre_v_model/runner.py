"""Top-level orchestration for base and sensitivity runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import polars as pl

from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.core.pipeline import (
    compute_energy_table,
    load_aligned_inputs,
    simulate_system,
    write_simulation_outputs,
    write_stage_outputs,
)
from seci_fdre_v_model.scenarios import build_case_rows, build_cross_table_rows
from seci_fdre_v_model.tender_inputs import generate_tender_input_files
from seci_fdre_v_model.workbook_export import write_summary_workbook

ProgressCallback = Callable[[str, float, str], None]


@dataclass(frozen=True)
class StudyRunResult:
    package_dir: Path
    workbook_path: Path


def run_full_study(
    config: ProjectConfig,
    *,
    dump_sections: bool = False,
    package_dir: str | Path | None = None,
    progress_callback: ProgressCallback | None = None,
) -> StudyRunResult:
    """Generate inputs, run the base study, and materialize all study artifacts."""

    def emit(stage: str, pct: float, detail: str) -> None:
        if progress_callback is not None:
            progress_callback(stage, round(pct, 1), detail)

    emit("Generate inputs", 0, "Generating tender-derived profile inputs")
    generate_tender_input_files(config)

    target_dir = _resolve_package_dir(config, package_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    package_name = config.project.plant_name

    emit("Base simulation", 4, "Running base simulation")
    base_result = simulate_system(
        config.simulation,
        progress_callback=_map_progress(emit, "Base simulation", 4, 28),
    )

    emit("Writing outputs", 30, "Writing base case outputs")
    write_simulation_outputs(base_result, config.simulation, target_dir, "base_case")
    pl.DataFrame(compute_energy_table(base_result.minute_flows)).write_csv(target_dir / "energy_table.csv")

    if dump_sections:
        emit("Writing outputs", 34, "Writing section outputs")
        aligned_input, context = load_aligned_inputs(config.simulation)
        write_stage_outputs(aligned_input, context, target_dir, "base_case")

    emit("Writing outputs", 36, "Writing study summary tables")
    pl.DataFrame([base_result.summary_metrics]).write_csv(target_dir / "base_summary.csv")

    emit("Sensitivity cases", 40, "Running named sensitivity cases")
    case_rows = build_case_rows(
        config,
        progress_callback=_scenario_progress(emit, "Sensitivity cases", 40, 62),
    )
    pl.DataFrame(case_rows).write_csv(target_dir / "cases_table.csv")

    emit("Sensitivity cross", 64, "Running sensitivity cross table")
    cross_rows = build_cross_table_rows(
        config,
        progress_callback=_scenario_progress(emit, "Sensitivity cross", 64, 92),
    )
    pl.DataFrame(cross_rows).write_csv(target_dir / "sensitivity_cross_table.csv")
    _build_profile_index(config).write_csv(target_dir / "profile_files_index.csv")

    workbook_output = target_dir / f"{package_name}.xlsx"
    emit("Workbook", 94, "Writing Excel summary workbook")
    workbook_path = write_summary_workbook(target_dir, output=workbook_output)
    emit("Done", 100, f"Study package written to {target_dir}")
    return StudyRunResult(package_dir=target_dir, workbook_path=workbook_path)


def _resolve_package_dir(config: ProjectConfig, package_dir: str | Path | None) -> Path:
    if package_dir is not None:
        return Path(package_dir).expanduser().resolve()
    return (Path(config.project.output_dir) / config.project.plant_name).expanduser().resolve()


def _map_progress(
    progress_callback: ProgressCallback,
    stage_name: str,
    start_pct: float,
    end_pct: float,
) -> ProgressCallback:
    span = max(end_pct - start_pct, 0.0)

    def emit(_: str, pct: float, detail: str) -> None:
        mapped_pct = start_pct + (pct / 100.0) * span
        progress_callback(stage_name, mapped_pct, detail)

    return emit


def _scenario_progress(
    progress_callback: ProgressCallback,
    stage_name: str,
    start_pct: float,
    end_pct: float,
) -> Callable[[int, int, str], None]:
    span = max(end_pct - start_pct, 0.0)

    def emit(current: int, total: int, case_id: str) -> None:
        denominator = max(total, 1)
        mapped_pct = start_pct + (current / denominator) * span
        progress_callback(stage_name, mapped_pct, f"Processed {case_id} ({current}/{denominator})")

    return emit


def _build_profile_index(config: ProjectConfig) -> pl.DataFrame:
    aux_path = (
        "derived at runtime (battery_state aux mode)"
        if config.simulation.load.uses_battery_state_aux
        else str(config.inputs.aux_power_path)
    )
    return pl.DataFrame(
        [
            {"profile_name": "Output Profile", "path": str(config.inputs.output_profile_path)},
            {"profile_name": "Output Profile 18-22", "path": str(config.inputs.output_profile_18_22_path)},
            {"profile_name": "Aux Power", "path": aux_path},
        ]
    )
