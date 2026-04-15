from __future__ import annotations

import csv
import io
import json
import time
from pathlib import Path

import polars as pl
import yaml

from seci_fdre_v_model.web.app import create_app
from seci_fdre_v_model.web.services import (
    create_run_snapshot,
    ensure_workspace_ready,
    load_project_config,
    save_project_form,
)


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

    health_response = client.get("/api/health")
    assert health_response.status_code == 200
    assert health_response.get_json() == {"status": "ok"}

    response = client.get("/inputs")
    assert response.status_code == 200
    assert b"Workspace Input Files" in response.data
    assert b"Ideal 1 MW workflow" in response.data
    assert (workspace_root / "inputs" / "solar.csv").exists()

    config_response = client.get("/config")
    assert config_response.status_code == 200
    assert b'name="study_profile"' in config_response.data
    assert b'<select name="simulation.preprocessing.gap_fill">' in config_response.data
    assert b'<select name="simulation.preprocessing.simulation_dtype">' in config_response.data
    assert b'name="simulation.load.profile_mode"' in config_response.data
    assert b'data-profile-mode-select' in config_response.data
    assert b'data-aux-mode-select' in config_response.data
    assert b'data-aux-battery-only' in config_response.data
    assert b'data-aux-static-only' in config_response.data
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

    start_response = client.post("/runs/study", data={"next": "/runs"}, follow_redirects=True)
    assert start_response.status_code == 200
    assert b"Study started (Workspace config)" in start_response.data

    run_id = None
    for _ in range(80):
        job_payload = client.get("/api/job-status").get_json()
        job = job_payload["job"]
        if job:
            assert job.get("study_profile") == "workspace"
            run_id = job["run_id"]
            if not job["is_active"]:
                break
        time.sleep(0.1)
    assert run_id is not None

    run_dir = workspace_root / "runs" / run_id
    assert (run_dir / "config" / "project.yaml").exists()
    assert (run_dir / "inputs" / "solar.csv").exists()
    assert (run_dir / "package" / "base_summary.csv").exists()
    assert (run_dir / "package" / "cases_table.csv").exists()
    assert (run_dir / "package" / "test_plant.xlsx").exists()

    dashboard_response = client.get(f"/runs/{run_id}")
    assert dashboard_response.status_code == 200
    assert run_id.encode("utf-8") in dashboard_response.data
    assert b"Energy Table" in dashboard_response.data
    assert b"Case for summary metrics" in dashboard_response.data

    cases_df = pl.read_csv(workspace_root / "runs" / run_id / "package" / "cases_table.csv")
    alt = cases_df.filter(pl.col("case_id") != "base").head(1)
    assert alt.height == 1
    alt_id = str(alt["case_id"][0])
    dash_alt = client.get(f"/runs/{run_id}?case_id={alt_id}")
    assert dash_alt.status_code == 200
    assert alt_id.encode("utf-8") in dash_alt.data
    assert b'selected="selected"' in dash_alt.data or b"selected" in dash_alt.data

    chart_response = client.get(f"/api/charts/{run_id}/base_case_minute_flows.parquet")
    assert chart_response.status_code == 200
    cards = chart_response.get_json()
    assert cards
    assert any(card["title"] == "Battery SOC" for card in cards)

    expand_response = client.get(f"/api/charts/{run_id}/base_case_minute_flows.parquet?expanded=1&index=2")
    assert expand_response.status_code == 200
    enlarged = expand_response.get_json()
    assert enlarged["title"] == "Battery SOC"
    assert "<svg" in enlarged["svg"]

    assert client.get(f"/api/charts/{run_id}/base_case_minute_flows.parquet?expanded=1").status_code == 400
    assert client.get(f"/api/charts/{run_id}/base_case_minute_flows.parquet?expanded=1&index=99").status_code == 404
    assert client.get("/api/charts/not-a-real-run/base_case_minute_flows.parquet").status_code == 404

    artifact_response = client.get(f"/runs/{run_id}/artifacts/base_summary.csv")
    assert artifact_response.status_code == 200
    assert b"plant_name" in artifact_response.data


def test_single_background_job_cancel_and_delete(tmp_path: Path, monkeypatch) -> None:
    source_config = _write_project_config(tmp_path)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    def fake_execute_run_snapshot(*args, progress_callback=None, **kwargs):
        for index in range(1, 101):
            if progress_callback is not None:
                progress_callback("Sensitivity cases", 40.0 + (index / 100.0) * 20.0, f"Processed case_{index} ({index}/10)")
            time.sleep(0.05)
        raise AssertionError("expected cancellation before fake run completed")

    monkeypatch.setattr("seci_fdre_v_model.web.app.execute_run_snapshot", fake_execute_run_snapshot)

    start_response = client.post("/runs/study", data={"next": "/runs"}, follow_redirects=True)
    assert start_response.status_code == 200
    assert b"Study started (Workspace config)" in start_response.data

    running_payload = client.get("/api/job-status").get_json()
    assert running_payload["job"] is not None
    assert running_payload["job"]["is_active"] is True
    assert running_payload["job"]["stage"] == "Sensitivity cases"
    assert running_payload["job"]["completed_cases"] is not None
    assert running_payload["job"]["total_cases"] == 10
    run_id = running_payload["job"]["run_id"]

    running_dashboard = client.get(f"/runs/{run_id}")
    assert running_dashboard.status_code == 200
    assert b"Background Job" in running_dashboard.data
    assert b"data-dashboard-job-card" in running_dashboard.data
    assert b"data-dashboard-job-cases" in running_dashboard.data

    second_start = client.post("/runs/study", data={"next": "/runs"}, follow_redirects=True)
    assert second_start.status_code == 200
    assert b"A study is already running" in second_start.data

    cancel_response = client.post("/jobs/current/cancel", data={"next": "/runs"}, follow_redirects=True)
    assert cancel_response.status_code == 200
    assert b"Cancellation requested" in cancel_response.data

    cancelled_payload = None
    for _ in range(80):
        cancelled_payload = client.get("/api/job-status").get_json()
        job = cancelled_payload["job"]
        if job is None or not job["is_active"]:
            break
        time.sleep(0.05)

    assert cancelled_payload is not None
    if cancelled_payload["job"] is not None:
        assert cancelled_payload["job"]["status"] == "cancelled"
    run_json = workspace_root / "runs" / run_id / "run.json"
    run_payload = json.loads(run_json.read_text(encoding="utf-8"))
    assert run_payload.get("status") == "cancelled"
    assert (workspace_root / "runs" / run_id).exists()

    delete_response = client.post(f"/runs/{run_id}/delete", data={"next": "/runs"}, follow_redirects=True)
    assert delete_response.status_code == 200
    assert b"Deleted run" in delete_response.data
    assert not (workspace_root / "runs" / run_id).exists()


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


def test_aux_mode_fields_toggle_and_save(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace = ensure_workspace_ready(tmp_path / ".workspace", source_config_path=source_config)

    dynamic_project = save_project_form(
        workspace,
        {
            "project.plant_name": "dynamic_aux_plant",
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
            "simulation.load.profile_mode": "template",
            "simulation.load.profile_template_id": "seci_fdre_v_amendment_03",
            "simulation.load.contracted_capacity_mw": "0.1",
            "simulation.load.aux_mode": "battery_state",
            "simulation.load.aux_charge_fraction": "0.03",
            "simulation.load.aux_discharge_fraction": "0.025",
            "simulation.load.aux_idle_fraction": "0.015",
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

    assert dynamic_project.simulation.load.aux_mode == "battery_state"
    assert dynamic_project.simulation.load.aux_charge_fraction == 0.03
    assert dynamic_project.simulation.load.aux_discharge_fraction == 0.025
    assert dynamic_project.simulation.load.aux_idle_fraction == 0.015


def test_dynamic_aux_inputs_page_disables_aux_csv_upload(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path, aux_mode="battery_state", include_aux_power_path=False)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    response = client.get("/inputs")
    assert response.status_code == 200
    assert b"Derived at run time from battery state" in response.data
    assert b"/inputs/upload/aux_power" not in response.data

    upload_response = client.post(
        "/inputs/upload/aux_power",
        data={"file": (io.BytesIO(b"timestamp,aux_power_kw\n2025-01-01 00:00,10\n"), "aux.csv")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert upload_response.status_code == 200
    assert b"aux_power.csv is not used in battery_state aux mode" in upload_response.data


def _write_project_config(
    tmp_path: Path,
    *,
    profile_mode: str = "template",
    output_profile_kw: float | None = None,
    aux_mode: str = "static_csv",
    include_aux_power_path: bool = True,
    aux_consumption_kw: float = 10.0,
    aux_charge_fraction: float = 0.03,
    aux_discharge_fraction: float = 0.025,
    aux_idle_fraction: float = 0.015,
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

    inputs_lines = [
        "inputs:",
        f"  solar_path: {solar_path}",
        f"  wind_path: {wind_path}",
        f"  output_profile_path: {tmp_path / 'output_profile.csv'}",
        f"  output_profile_18_22_path: {tmp_path / 'output_profile_18_22.csv'}",
    ]
    if include_aux_power_path:
        inputs_lines.append(f"  aux_power_path: {tmp_path / 'aux_power.csv'}")

    load_lines = [
        "  load:",
        f"    profile_mode: {profile_mode}",
    ]
    if profile_mode == "template":
        load_lines.extend(
            [
                "    profile_template_id: seci_fdre_v_amendment_03",
                "    contracted_capacity_mw: 0.1",
            ]
        )
    else:
        load_lines.append(f"    output_profile_kw: {float(output_profile_kw or 0.0)}")

    if aux_mode == "battery_state":
        load_lines.extend(
            [
                "    aux_mode: battery_state",
                f"    aux_charge_fraction: {aux_charge_fraction}",
                f"    aux_discharge_fraction: {aux_discharge_fraction}",
                f"    aux_idle_fraction: {aux_idle_fraction}",
            ]
        )
    else:
        load_lines.append(f"    aux_consumption_kw: {aux_consumption_kw}")

    config_path = tmp_path / "project.yaml"
    config_path.write_text(
        "\n".join(
            [
                "project:",
                "  plant_name: test_plant",
                f"  output_dir: {tmp_path / 'output'}",
                '  simulation_start: "2025-01-01 00:00"',
                '  simulation_end: "2025-01-01 00:05"',
                *inputs_lines,
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
                *load_lines,
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


def test_api_aligned_energy_report(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    report = client.get("/api/aligned-energy-report?excess_fraction=0.1")
    assert report.status_code == 200
    body = report.get_json()
    assert body["ok"] is True
    assert body["summary"]["minutes"] == 6
    assert "solar_kwh" in body["summary"]
    assert "suggestions" in body
    assert body["suggestions"]["annual_load_to_generation_ratio"] > 0
    assert "uniform_renewable_scale" in body["suggestions"]


def test_api_aligned_energy_report_dynamic_aux_note(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path, aux_mode="battery_state", include_aux_power_path=False)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    report = client.get("/api/aligned-energy-report")
    body = report.get_json()

    assert report.status_code == 200
    assert body["ok"] is True
    assert "Idle-state approximation" in body["summary"]["aux_note"]


def test_api_config_form_preview_workspace_vs_ideal(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    ws = client.get("/api/config-form-preview?study_profile=workspace").get_json()
    assert ws["study_profile"] == "workspace"
    assert ws["editable"] is True
    assert ws["fields"]["project.plant_name"] == "test_plant"

    ideal = client.get("/api/config-form-preview?study_profile=ideal_1mw").get_json()
    assert ideal["study_profile"] == "ideal_1mw"
    assert ideal["editable"] is False
    assert ideal["fields"]["project.plant_name"] == "ideal_1mw_fdre"


def test_create_run_snapshot_ideal_profile_uses_bundled_plant_name(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace_root = tmp_path / ".workspace"
    state = ensure_workspace_ready(workspace_root, source_config_path=source_config)
    _run_id, run_dir, config_path, _package_dir = create_run_snapshot(state, study_profile="ideal_1mw")
    snapshot = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert snapshot["project"]["plant_name"] == "ideal_1mw_fdre"
    assert snapshot["inputs"]["solar_path"] == "../inputs/solar.csv"
    assert (run_dir / "inputs" / "solar.csv").exists()


def test_create_run_snapshot_dynamic_aux_skips_aux_input_copy(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path, aux_mode="battery_state", include_aux_power_path=False)
    workspace_root = tmp_path / ".workspace"
    state = ensure_workspace_ready(workspace_root, source_config_path=source_config)

    _run_id, run_dir, config_path, _package_dir = create_run_snapshot(state, study_profile="workspace")
    snapshot = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert snapshot["simulation"]["load"]["aux_mode"] == "battery_state"
    assert not (run_dir / "inputs" / "aux_power.csv").exists()


def test_apply_ideal_preset_and_ideal_tile_web(tmp_path: Path) -> None:
    source_config = _write_project_config(tmp_path)
    workspace_root = tmp_path / ".workspace"
    app = create_app(workspace_root=workspace_root, source_config_path=source_config)
    client = app.test_client()

    tile = client.post("/runs/ideal-tile-profiles", data={"solar_scale": "1", "wind_scale": "1"}, follow_redirects=True)
    assert tile.status_code == 200
    solar_csv = workspace_root / "inputs" / "solar.csv"
    assert solar_csv.exists()
    line_count = solar_csv.read_text(encoding="utf-8").count("\n")
    assert line_count >= 7

    apply_resp = client.post("/runs/apply-ideal-preset", follow_redirects=True)
    assert apply_resp.status_code == 200

    workspace = ensure_workspace_ready(workspace_root, source_config_path=source_config)
    project = load_project_config(workspace)
    assert project.simulation.battery.nominal_power_kw == 1000.0
    assert project.sensitivity.wind_multipliers == [1.0]
