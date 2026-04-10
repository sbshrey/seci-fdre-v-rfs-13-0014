from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from seci_fdre_v_model.web.app import create_app
from seci_fdre_v_model.web.services import ensure_workspace_ready, load_project_config, save_project_form


def test_workspace_bootstrap_and_form_save(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace = ensure_workspace_ready(tmp_path / ".workspace", source_config_path=source_config)

    assert workspace.config_path.exists()
    assert (workspace.inputs_dir / "solar.csv").exists()
    assert (workspace.inputs_dir / "wind.csv").exists()
    assert (workspace.inputs_dir / "output_profile.csv").exists()

    saved = save_project_form(
        workspace,
        {
            "project.plant_name": "updated_plant",
            "project.simulation_start": "2025-01-01 00:00",
            "project.simulation_end": "2025-01-01 00:05",
            "simulation.data.solar_enabled": "on",
            "simulation.data.wind_enabled": "on",
            "simulation.preprocessing.frequency": "1m",
            "simulation.preprocessing.gap_fill": "zero",
            "simulation.preprocessing.max_interpolation_gap_minutes": "15",
            "simulation.preprocessing.align_to_full_year": "",
            "simulation.preprocessing.simulation_dtype": "float64",
            "simulation.grid.export_limit_kw": "1000.0",
            "simulation.grid.import_limit_kw": "",
            "simulation.load.profile_mode": "template",
            "simulation.load.profile_template_id": "seci_fdre_v_amendment_03",
            "simulation.load.contracted_capacity_mw": "0.1",
            "simulation.load.output_profile_kw": "",
            "simulation.load.aux_consumption_kw": "10.0",
            "simulation.battery.nominal_power_kw": "100.0",
            "simulation.battery.duration_hours": "1.0",
            "simulation.battery.charge_efficiency": "1.0",
            "simulation.battery.discharge_efficiency": "1.0",
            "simulation.battery.degradation_per_cycle": "0.0",
            "simulation.battery.initial_soc_fraction": "0.5",
            "simulation.battery.min_soc_fraction": "0.0",
            "simulation.battery.max_soc_fraction": "1.0",
            "simulation.battery.charge_loss_table": "0.0: 0.0\n1.0: 0.0",
            "simulation.battery.discharge_loss_table": "0.0: 0.0\n1.0: 0.0",
            "sensitivity.wind_multipliers": "1.0, 1.1",
            "sensitivity.solar_multipliers": "1.0, 1.1",
            "sensitivity.profile_multipliers": "1.0, 1.1",
            "sensitivity.battery_capacity_kwh_values": "100.0, 200.0",
            "sensitivity.battery_duration_hour_values": "1.0, 2.0",
        },
    )

    assert saved.project.plant_name == "updated_plant"
    reloaded = load_project_config(workspace)
    assert reloaded.inputs.solar_path == workspace.inputs_dir / "solar.csv"
    assert reloaded.inputs.output_profile_path == workspace.inputs_dir / "output_profile.csv"


def test_web_control_room_flow(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    response = client.get("/inputs")
    assert response.status_code == 200
    assert b"Workspace Input Files" in response.data
    assert (workspace_root / "inputs" / "solar.csv").exists()

    config_response = client.get("/config")
    assert config_response.status_code == 200
    assert b'<select name="simulation.preprocessing.gap_fill">' in config_response.data
    assert b'<select name="simulation.preprocessing.simulation_dtype">' in config_response.data
    assert b'name="simulation.load.profile_mode"' in config_response.data
    assert b'data-profile-mode-select' in config_response.data
    assert b'data-template-only' in config_response.data
    assert b'data-flat-only' in config_response.data
    assert b'disabled' in config_response.data
    assert b'Linear interpolate' in config_response.data
    assert b'float64' in config_response.data

    upload_response = client.post(
        "/inputs/upload/solar",
        data={
            "file": (
                io.BytesIO(b"timestamp,Power in KW\n01/01/2025 00:00,80\n01/01/2025 00:01,90\n"),
                "solar_upload.csv",
            )
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert upload_response.status_code == 200
    assert b"Input file uploaded" in upload_response.data

    generate_response = client.post("/runs/generate", follow_redirects=True)
    assert generate_response.status_code == 200
    assert (workspace_root / "inputs" / "output_profile.csv").exists()
    assert (workspace_root / "inputs" / "aux_power.csv").exists()

    stream_response = client.post("/runs/study", buffered=False)
    assert stream_response.status_code == 200
    payloads = []
    for chunk in stream_response.response:
        if not chunk:
            continue
        for line in chunk.decode("utf-8").splitlines():
            if line.strip():
                payloads.append(json.loads(line))
    done = next(item for item in payloads if item.get("done"))
    redirect = done["redirect"]
    run_id = redirect.rstrip("/").split("/")[-1]

    run_dir = workspace_root / "runs" / run_id
    assert (run_dir / "config" / "project.yaml").exists()
    assert (run_dir / "inputs" / "solar.csv").exists()
    assert (run_dir / "package" / "base_summary.csv").exists()
    assert (run_dir / "package" / "cases_table.csv").exists()
    assert (run_dir / "package" / "test_plant.xlsx").exists()
    assert (run_dir / "package" / "test_plant.zip").exists()

    dashboard_response = client.get(f"/runs/{run_id}")
    assert dashboard_response.status_code == 200
    assert run_id.encode("utf-8") in dashboard_response.data
    assert b"Energy Table" in dashboard_response.data

    chart_response = client.get(f"/api/charts/{run_id}/base_case_minute_flows.parquet")
    assert chart_response.status_code == 200
    cards = chart_response.get_json()
    assert cards
    assert any(card["title"] == "Battery SOC" for card in cards)

    artifact_response = client.get(f"/runs/{run_id}/artifacts/base_summary.csv")
    assert artifact_response.status_code == 200
    assert b"plant_name" in artifact_response.data


def test_profile_mode_fields_toggle_and_save(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace = ensure_workspace_ready(tmp_path / ".workspace", source_config_path=source_config)

    flat_project = save_project_form(
        workspace,
        {
            "project.plant_name": "flat_plant",
            "project.simulation_start": "2025-01-01 00:00",
            "project.simulation_end": "2025-01-01 00:05",
            "simulation.data.solar_enabled": "on",
            "simulation.data.wind_enabled": "on",
            "simulation.preprocessing.frequency": "1m",
            "simulation.preprocessing.gap_fill": "zero",
            "simulation.preprocessing.max_interpolation_gap_minutes": "15",
            "simulation.preprocessing.simulation_dtype": "float64",
            "simulation.grid.export_limit_kw": "1000.0",
            "simulation.grid.import_limit_kw": "",
            "simulation.load.profile_mode": "flat",
            "simulation.load.output_profile_kw": "275.0",
            "simulation.load.aux_consumption_kw": "10.0",
            "simulation.battery.nominal_power_kw": "100.0",
            "simulation.battery.duration_hours": "1.0",
            "simulation.battery.charge_efficiency": "1.0",
            "simulation.battery.discharge_efficiency": "1.0",
            "simulation.battery.degradation_per_cycle": "0.0",
            "simulation.battery.initial_soc_fraction": "0.5",
            "simulation.battery.min_soc_fraction": "0.0",
            "simulation.battery.max_soc_fraction": "1.0",
            "simulation.battery.charge_loss_table": "0.0: 0.0\n1.0: 0.0",
            "simulation.battery.discharge_loss_table": "0.0: 0.0\n1.0: 0.0",
            "sensitivity.wind_multipliers": "1.0, 1.1",
            "sensitivity.solar_multipliers": "1.0, 1.1",
            "sensitivity.profile_multipliers": "1.0, 1.1",
            "sensitivity.battery_capacity_kwh_values": "100.0, 200.0",
            "sensitivity.battery_duration_hour_values": "1.0, 2.0",
        },
    )

    assert flat_project.simulation.load.profile_mode == "flat"
    assert flat_project.simulation.load.output_profile_kw == 275.0
    assert flat_project.simulation.load.profile_template_id == "seci_fdre_v_amendment_03"
    assert flat_project.simulation.load.contracted_capacity_mw == 0.1


def _write_project_config(tmp_path: Path) -> Path:
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
                '  simulation_start: "2025-01-01 00:00"',
                '  simulation_end: "2025-01-01 00:05"',
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
