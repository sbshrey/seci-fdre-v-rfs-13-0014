"""Tests for aligned pre-BESS energy reporting."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from seci_fdre_v_model.aligned_energy_report import (
    AlignedEnergySummary,
    format_aligned_energy_report,
    suggest_alignment_scales,
    summarize_aligned_frame,
    summarize_aligned_inputs,
)
from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.ideal_year_profiles import write_tiled_year_profiles


def test_suggest_alignment_scales_load_heavy() -> None:
    summary = AlignedEnergySummary(
        minutes=525_600,
        solar_kwh=2_400.0,
        wind_kwh=1_200.0,
        generation_kwh=3_600.0,
        output_profile_kwh=5_500_000.0,
        aux_kwh=200_000.0,
        consumption_kwh=5_700_000.0,
        net_generation_minus_load_kwh=-5_696_400.0,
        surplus_minutes=0,
        deficit_minutes=525_600,
    )
    sug = suggest_alignment_scales(
        summary,
        solar_multiplier=1.0,
        wind_multiplier=1.0,
        profile_multiplier=1.0,
        excess_fraction=0.08,
        renewable_scale_cap=500.0,
    )
    assert sug["annual_load_to_generation_ratio"] == pytest.approx(5_700_000 / 3_600, rel=1e-6)
    assert sug["uniform_renewable_scale"] == 500.0
    assert sug["renewable_scale_cap_hit"] is True
    assert sug["profile_multiplier_scale"] < 1.0


def test_summarize_aligned_frame_basic() -> None:
    df = pl.DataFrame(
        {
            "solar_kw": [60.0, 0.0, 120.0],
            "wind_kw": [0.0, 60.0, 60.0],
            "total_generation_kw": [60.0, 60.0, 180.0],
            "output_profile_kw": [100.0, 100.0, 100.0],
            "aux_consumption_kw": [0.0, 0.0, 0.0],
            "total_consumption_kw": [100.0, 100.0, 100.0],
        }
    )
    s = summarize_aligned_frame(df)
    assert s.minutes == 3
    assert s.solar_kwh == 3.0  # 180 kW·min / 60
    assert s.wind_kwh == 2.0
    assert s.generation_kwh == 5.0
    assert s.consumption_kwh == 5.0
    assert s.surplus_minutes == 1
    assert s.deficit_minutes == 2
    text = format_aligned_energy_report(s, plant_name="test")
    assert "Solar + wind" in text
    assert "test" in text


def test_summarize_aligned_inputs_smoke(tmp_path: Path) -> None:
    """End-to-end smoke: tile two days, minimal yaml, aligned report runs."""
    repo = Path(__file__).resolve().parents[1]
    solar_seed = repo / "data" / "Solar_2025-01-01_data_.csv"
    wind_seed = repo / "data" / "Wind_2025_01-01_data_.csv"
    solar_out = tmp_path / "solar_year.csv"
    wind_out = tmp_path / "wind_year.csv"
    write_tiled_year_profiles(
        simulation_start=datetime(2025, 1, 1, 0, 0),
        simulation_end=datetime(2025, 1, 2, 23, 59),
        solar_source=solar_seed,
        wind_source=wind_seed,
        solar_out=solar_out,
        wind_out=wind_out,
    )

    yaml_text = f"""
project:
  plant_name: test_ideal
  output_dir: output
  simulation_start: "2025-01-01 00:00"
  simulation_end: "2025-01-02 23:59"

inputs:
  solar_path: {solar_out.as_posix()}
  wind_path: {wind_out.as_posix()}
  output_profile_path: {(repo / "data" / "seci_fdre_v_amendment_03_output_profile.csv").as_posix()}
  output_profile_18_22_path: {(repo / "data" / "seci_fdre_v_amendment_03_output_profile_18_22.csv").as_posix()}
  aux_power_path: {(repo / "data" / "aux_power_profile.csv").as_posix()}

simulation:
  data:
    solar_enabled: true
    wind_enabled: true
  preprocessing:
    frequency: 1m
    gap_fill: zero
    max_interpolation_gap_minutes: 15
    align_to_full_year: false
    simulation_dtype: float64
  grid:
    export_limit_kw: 1000.0
    import_limit_kw: null
  load:
    profile_mode: template
    profile_template_id: seci_fdre_v_amendment_03
    contracted_capacity_mw: 1.0
    aux_consumption_kw: 20.0
  battery:
    nominal_power_kw: 500.0
    duration_hours: 2.0
    initial_soc_fraction: 0.5

sensitivity:
  wind_multipliers: [1.0]
  solar_multipliers: [1.0]
  profile_multipliers: [1.0]
  battery_capacity_kwh_values: [1000.0]
  battery_duration_hour_values: [2.0]
"""
    cfg_path = tmp_path / "project.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")
    project = ProjectConfig.from_yaml(cfg_path)
    summary = summarize_aligned_inputs(project.simulation)
    assert summary.minutes > 2000
    assert summary.solar_kwh > 0
    assert summary.consumption_kwh > 0


def test_summarize_aligned_inputs_dynamic_aux_marks_idle_approximation(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[1]
    solar_seed = repo / "data" / "Solar_2025-01-01_data_.csv"
    wind_seed = repo / "data" / "Wind_2025_01-01_data_.csv"
    solar_out = tmp_path / "solar_year.csv"
    wind_out = tmp_path / "wind_year.csv"
    write_tiled_year_profiles(
        simulation_start=datetime(2025, 1, 1, 0, 0),
        simulation_end=datetime(2025, 1, 1, 23, 59),
        solar_source=solar_seed,
        wind_source=wind_seed,
        solar_out=solar_out,
        wind_out=wind_out,
    )

    yaml_text = f"""
project:
  plant_name: test_dynamic_aux
  output_dir: output
  simulation_start: "2025-01-01 00:00"
  simulation_end: "2025-01-01 23:59"

inputs:
  solar_path: {solar_out.as_posix()}
  wind_path: {wind_out.as_posix()}
  output_profile_path: {(repo / "data" / "seci_fdre_v_amendment_03_output_profile.csv").as_posix()}
  output_profile_18_22_path: {(repo / "data" / "seci_fdre_v_amendment_03_output_profile_18_22.csv").as_posix()}

simulation:
  data:
    solar_enabled: true
    wind_enabled: true
  preprocessing:
    frequency: 1m
    gap_fill: zero
    max_interpolation_gap_minutes: 15
    align_to_full_year: false
    simulation_dtype: float64
  grid:
    export_limit_kw: 1000.0
    import_limit_kw: null
  load:
    profile_mode: template
    profile_template_id: seci_fdre_v_amendment_03
    contracted_capacity_mw: 1.0
    aux_mode: battery_state
    aux_charge_fraction: 0.03
    aux_discharge_fraction: 0.025
    aux_idle_fraction: 0.015
  battery:
    nominal_power_kw: 500.0
    duration_hours: 2.0
    initial_soc_fraction: 0.5

sensitivity:
  wind_multipliers: [1.0]
  solar_multipliers: [1.0]
  profile_multipliers: [1.0]
  battery_capacity_kwh_values: [1000.0]
  battery_duration_hour_values: [2.0]
"""
    cfg_path = tmp_path / "project_dynamic.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")

    project = ProjectConfig.from_yaml(cfg_path)
    summary = summarize_aligned_inputs(project.simulation)

    assert summary.aux_note == "Idle-state approximation for battery_state aux mode."
    assert summary.aux_kwh > 0
