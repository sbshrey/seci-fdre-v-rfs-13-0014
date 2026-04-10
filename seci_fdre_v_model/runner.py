"""Top-level orchestration for base and sensitivity runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.core.pipeline import compute_energy_table, load_aligned_inputs, simulate_system, write_simulation_outputs, write_stage_outputs
from seci_fdre_v_model.scenarios import build_case_rows, build_cross_table_rows
from seci_fdre_v_model.tender_inputs import generate_tender_input_files
from seci_fdre_v_model.workbook_export import export_study_workbook


@dataclass(frozen=True)
class StudyRunResult:
    package_dir: Path
    workbook_path: Path


def run_full_study(config: ProjectConfig, *, dump_sections: bool = False) -> StudyRunResult:
    generate_tender_input_files(config)

    package_dir = Path(config.project.output_dir) / config.project.plant_name
    package_dir.mkdir(parents=True, exist_ok=True)

    base_result = simulate_system(config.simulation)
    write_simulation_outputs(base_result, package_dir, "base_case")
    pl.DataFrame(compute_energy_table(base_result.minute_flows)).write_csv(package_dir / "energy_table.csv")

    if dump_sections:
        aligned_input, context = load_aligned_inputs(config.simulation)
        write_stage_outputs(aligned_input, context, package_dir, "base_case")

    pl.DataFrame([base_result.summary_metrics]).write_csv(package_dir / "base_summary.csv")
    pl.DataFrame(build_case_rows(config)).write_csv(package_dir / "cases_table.csv")
    pl.DataFrame(build_cross_table_rows(config)).write_csv(package_dir / "sensitivity_cross_table.csv")
    _build_profile_index(config).write_csv(package_dir / "profile_files_index.csv")

    workbook_path, _ = export_study_workbook(package_dir)
    return StudyRunResult(package_dir=package_dir, workbook_path=workbook_path)


def _build_profile_index(config: ProjectConfig) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"profile_name": "Output Profile", "path": str(config.inputs.output_profile_path)},
            {"profile_name": "Output Profile 18-22", "path": str(config.inputs.output_profile_18_22_path)},
            {"profile_name": "Aux Power", "path": str(config.inputs.aux_power_path)},
        ]
    )
