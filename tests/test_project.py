from __future__ import annotations

import csv
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import polars as pl
import pytest

from seci_fdre_v_model.cli import main
from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.core.pipeline import _balance_tolerance_for_dtype
from seci_fdre_v_model.flows.section_outputs import _identity_tolerance
from seci_fdre_v_model.runner import run_full_study
from seci_fdre_v_model.tender_inputs import generate_tender_input_files


def test_generate_tender_input_files_creates_separate_profile_and_aux_files(tmp_path: Path) -> None:
    config_path = _write_project_config(tmp_path)
    project = ProjectConfig.from_yaml(config_path)

    written = generate_tender_input_files(project)

    assert len(written) == 3
    output_df = pl.read_csv(project.inputs.output_profile_path, try_parse_dates=True)
    aux_df = pl.read_csv(project.inputs.aux_power_path, try_parse_dates=True)
    evening_df = pl.read_csv(project.inputs.output_profile_18_22_path, try_parse_dates=True)

    assert "output_profile_kw" in output_df.columns
    assert "aux_power_kw" in aux_df.columns
    assert "output_profile_18_22_kw" in evening_df.columns
    assert output_df.height == 6
    assert aux_df["aux_power_kw"].to_list() == [10.0] * 6
    assert evening_df["output_profile_18_22_kw"].sum() == 0.0


def test_run_full_study_writes_cases_cross_table_and_workbook(tmp_path: Path) -> None:
    config_path = _write_project_config(tmp_path)
    project = ProjectConfig.from_yaml(config_path)

    result = run_full_study(project, dump_sections=False)

    package_dir = result.package_dir
    assert (package_dir / "base_summary.csv").exists()
    assert (package_dir / "cases_table.csv").exists()
    assert (package_dir / "sensitivity_cross_table.csv").exists()
    assert (package_dir / "profile_files_index.csv").exists()
    assert result.workbook_path.exists()

    cases_df = pl.read_csv(package_dir / "cases_table.csv")
    cross_df = pl.read_csv(package_dir / "sensitivity_cross_table.csv")
    assert cases_df.height == 6
    assert cross_df.height == 32
    assert "grid_import_kw_min" in cross_df.columns
    assert _sheet_names(result.workbook_path) == [
        "Base Summary",
        "Energy Table",
        "Cases",
        "Sensitivity Cross",
        "Profile Files Index",
    ]

    base_metrics = pl.read_csv(package_dir / "base_summary.csv").to_dicts()[0]
    assert base_metrics.get("generation_equals_solar_plus_wind") == 1
    et = pl.read_csv(package_dir / "energy_table.csv")
    solar_sum = float(et.filter(pl.col("element") == "Solar Power")["value_kw_min"][0])
    wind_sum = float(et.filter(pl.col("element") == "Wind Power")["value_kw_min"][0])
    assert float(base_metrics["solar_kw_min_sum"]) == pytest.approx(solar_sum)
    assert float(base_metrics["wind_kw_min_sum"]) == pytest.approx(wind_sum)


def test_evening_profile_file_is_nonzero_only_between_18_and_22(tmp_path: Path) -> None:
    config_path = _write_project_config(
        tmp_path,
        simulation_start="2025-01-01 17:58",
        simulation_end="2025-01-01 22:01",
    )
    project = ProjectConfig.from_yaml(config_path)

    generate_tender_input_files(project)
    evening_df = pl.read_csv(project.inputs.output_profile_18_22_path, try_parse_dates=True).with_columns(
        pl.col("timestamp").dt.hour().alias("hour")
    )

    nonzero_hours = (
        evening_df.with_columns(pl.col("timestamp").dt.hour().alias("hour"))
        .filter(pl.col("output_profile_18_22_kw") > 0)
        .get_column("hour")
        .unique()
        .sort()
        .to_list()
    )
    assert nonzero_hours == [18, 19, 20, 21]
    assert evening_df.filter(pl.col("hour").is_between(0, 17)).select(pl.col("output_profile_18_22_kw").sum()).item() == 0.0


def test_cli_generate_and_run_workflow(tmp_path: Path) -> None:
    config_path = _write_project_config(tmp_path)

    assert main(["generate-input-files", "--config", str(config_path)]) == 0
    assert main(["run", "--config", str(config_path)]) == 0

    package_dir = tmp_path / "output" / "test_plant"
    assert (package_dir / "cases_table.csv").exists()
    assert (package_dir / "sensitivity_cross_table.csv").exists()
    assert (package_dir / "test_plant.xlsx").exists()


def test_float32_uses_relaxed_identity_tolerance() -> None:
    assert _identity_tolerance("float32") == 1e-2
    assert _identity_tolerance("float64") == 1e-3
    assert _balance_tolerance_for_dtype("float32") == 1e-2
    assert _balance_tolerance_for_dtype("float64") == 1e-3


def test_parallel_cross_table_matches_sequential(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from seci_fdre_v_model.scenarios import build_cross_table_rows

    config_path = _write_project_config(tmp_path)
    project = ProjectConfig.from_yaml(config_path)
    generate_tender_input_files(project)

    monkeypatch.setenv("SECI_FDRE_V_SCENARIO_WORKERS", "1")
    sequential = build_cross_table_rows(project)

    monkeypatch.setenv("SECI_FDRE_V_SCENARIO_WORKERS", "4")
    parallel = build_cross_table_rows(project)

    key = lambda row: str(row["case_id"])
    assert sorted(sequential, key=key) == sorted(parallel, key=key)


def _write_project_config(
    tmp_path: Path,
    *,
    simulation_start: str = "2025-01-01 00:00",
    simulation_end: str = "2025-01-01 00:05",
) -> Path:
    solar_path = tmp_path / "solar.csv"
    wind_path = tmp_path / "wind.csv"
    _write_csv(
        solar_path,
        ["timestamp", "Power in KW"],
        [
            ["01/01/2025 00:00", "50"],
            ["01/01/2025 00:01", "60"],
            ["01/01/2025 00:02", "40"],
            ["01/01/2025 00:03", "10"],
            ["01/01/2025 00:04", "0"],
            ["01/01/2025 00:05", "0"],
        ],
    )
    _write_csv(
        wind_path,
        ["time stamp", "Power in KW"],
        [
            ["2025-01-01 00:00", "20"],
            ["2025-01-01 00:01", "20"],
            ["2025-01-01 00:02", "20"],
            ["2025-01-01 00:03", "20"],
            ["2025-01-01 00:04", "20"],
            ["2025-01-01 00:05", "20"],
        ],
    )

    config_path = tmp_path / "project.yaml"
    config_path.write_text(
        "\n".join(
            [
                "project:",
                "  plant_name: test_plant",
                f"  output_dir: {tmp_path / 'output'}",
                f'  simulation_start: "{simulation_start}"',
                f'  simulation_end: "{simulation_end}"',
                "inputs:",
                f"  solar_path: {solar_path}",
                f"  wind_path: {wind_path}",
                f"  output_profile_path: {tmp_path / 'output_profile.csv'}",
                f"  output_profile_18_22_path: {tmp_path / 'output_profile_18_22.csv'}",
                f"  aux_power_path: {tmp_path / 'aux_power.csv'}",
                "simulation:",
                "  data:",
                "    solar_enabled: true",
                "    wind_enabled: true",
                "  preprocessing:",
                "    frequency: 1m",
                "    gap_fill: zero",
                "    max_interpolation_gap_minutes: 15",
                "    align_to_full_year: false",
                "    simulation_dtype: float64",
                "  grid:",
                "    export_limit_kw: 1000.0",
                "    import_limit_kw: null",
                "  load:",
                "    profile_mode: template",
                "    profile_template_id: seci_fdre_v_amendment_03",
                "    contracted_capacity_mw: 0.1",
                "    aux_consumption_kw: 10.0",
                "  battery:",
                "    nominal_power_kw: 100.0",
                "    duration_hours: 1.0",
                "    initial_soc_fraction: 0.5",
                "    charge_efficiency: 1.0",
                "    discharge_efficiency: 1.0",
                "    degradation_per_cycle: 0.0",
                "    charge_loss_table:",
                "      0.0: 0.0",
                "      1.0: 0.0",
                "    discharge_loss_table:",
                "      0.0: 0.0",
                "      1.0: 0.0",
                "sensitivity:",
                "  wind_multipliers: [1.0, 1.1]",
                "  solar_multipliers: [1.0, 1.1]",
                "  profile_multipliers: [1.0, 1.1]",
                "  battery_capacity_kwh_values: [100.0, 200.0]",
                "  battery_duration_hour_values: [1.0, 2.0]",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _sheet_names(workbook_path: Path) -> list[str]:
    with zipfile.ZipFile(workbook_path) as workbook_zip:
        workbook_xml = workbook_zip.read("xl/workbook.xml")

    root = ET.fromstring(workbook_xml)
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    return [sheet.attrib["name"] for sheet in root.findall("main:sheets/main:sheet", ns)]
