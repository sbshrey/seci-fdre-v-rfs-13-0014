"""Workspace, reporting, and chart helpers for the local control room."""

from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import math
import os
import secrets
import shutil
import tempfile
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import polars as pl
import yaml

from seci_fdre_v_model.aligned_energy_report import suggest_alignment_scales, summarize_aligned_inputs
from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.ideal_year_profiles import write_tiled_year_profiles
from seci_fdre_v_model.runtime import bundled_root, repo_root, resolve_seed_source_config_path
from seci_fdre_v_model.data.loaders import (
    AUX_POWER_COLUMN,
    PROFILE_POWER_COLUMN,
    SOLAR_POWER_COLUMN,
    SOLAR_TIMESTAMP_COLUMN,
    WIND_POWER_COLUMN,
    WIND_TIMESTAMP_COLUMN,
)
from seci_fdre_v_model.runner import run_full_study
from seci_fdre_v_model.tender_inputs import generate_tender_input_files
from seci_fdre_v_model.web.models import (
    ChartCard,
    EnergyTableRow,
    ManagedInput,
    MetricCard,
    RunArtifactIndex,
    RunRecord,
    TablePreview,
    WorkspaceState,
)

ProgressCallback = Callable[[str, float, str], None]


class StudyCancelledError(RuntimeError):
    """Raised when a background study is cancelled by the user."""


@dataclass(frozen=True)
class ManagedInputSpec:
    key: str
    label: str
    canonical_name: str
    description: str
    expected_headers: tuple[str, ...]
    generated: bool = False


MANAGED_INPUT_SPECS: tuple[ManagedInputSpec, ...] = (
    ManagedInputSpec(
        key="solar",
        label="Solar Generation",
        canonical_name="solar.csv",
        description="Minute or source-resolution solar generation input.",
        expected_headers=(SOLAR_TIMESTAMP_COLUMN, SOLAR_POWER_COLUMN),
    ),
    ManagedInputSpec(
        key="wind",
        label="Wind Generation",
        canonical_name="wind.csv",
        description="Minute or source-resolution wind generation input.",
        expected_headers=(WIND_TIMESTAMP_COLUMN, WIND_POWER_COLUMN),
    ),
    ManagedInputSpec(
        key="output_profile",
        label="Output Profile",
        canonical_name="output_profile.csv",
        description="Tender-derived or uploaded output profile.",
        expected_headers=("timestamp", PROFILE_POWER_COLUMN),
        generated=True,
    ),
    ManagedInputSpec(
        key="output_profile_18_22",
        label="Output Profile 18-22",
        canonical_name="output_profile_18_22.csv",
        description="Evening-only tender profile derived from the main output profile.",
        expected_headers=("timestamp", "output_profile_18_22_kw"),
        generated=True,
    ),
    ManagedInputSpec(
        key="aux_power",
        label="Aux Power",
        canonical_name="aux_power.csv",
        description="Auxiliary consumption profile.",
        expected_headers=("timestamp", AUX_POWER_COLUMN),
        generated=True,
    ),
)

SPEC_BY_KEY = {spec.key: spec for spec in MANAGED_INPUT_SPECS}

BOOL_KEYS = {
    "simulation.data.solar_enabled",
    "simulation.data.wind_enabled",
    "simulation.preprocessing.align_to_full_year",
}
FLOAT_KEYS = {
    "simulation.grid.export_limit_kw",
    "simulation.load.output_profile_kw",
    "simulation.load.aux_consumption_kw",
    "simulation.load.contracted_capacity_mw",
    "simulation.battery.nominal_power_kw",
    "simulation.battery.duration_hours",
    "simulation.battery.charge_efficiency",
    "simulation.battery.discharge_efficiency",
    "simulation.battery.degradation_per_cycle",
    "simulation.battery.initial_soc_fraction",
    "simulation.battery.min_soc_fraction",
    "simulation.battery.max_soc_fraction",
}
NULLABLE_FLOAT_KEYS = {
    "simulation.grid.import_limit_kw",
    "simulation.load.output_profile_kw",
    "simulation.load.contracted_capacity_mw",
}
INT_KEYS = {"simulation.preprocessing.max_interpolation_gap_minutes"}
LIST_FLOAT_KEYS = {
    "sensitivity.wind_multipliers",
    "sensitivity.solar_multipliers",
    "sensitivity.profile_multipliers",
    "sensitivity.battery_capacity_kwh_values",
    "sensitivity.battery_duration_hour_values",
}
TEXT_KEYS = {
    "project.plant_name",
    "project.simulation_start",
    "project.simulation_end",
    "simulation.preprocessing.frequency",
    "simulation.preprocessing.gap_fill",
    "simulation.preprocessing.simulation_dtype",
    "simulation.load.profile_mode",
    "simulation.load.profile_template_id",
}
TABLE_KEYS = {
    "simulation.battery.charge_loss_table",
    "simulation.battery.discharge_loss_table",
}
FORM_SKIP_KEYS = {"csrf_token"}

STUDY_PROFILE_WORKSPACE = "workspace"
STUDY_PROFILE_IDEAL_1MW = "ideal_1mw"


def normalize_study_profile(value: str | None) -> str:
    """Return a supported study snapshot profile id (defaults to workspace)."""
    if (value or "").strip() == STUDY_PROFILE_IDEAL_1MW:
        return STUDY_PROFILE_IDEAL_1MW
    return STUDY_PROFILE_WORKSPACE


def study_config_payload_for_snapshot(state: WorkspaceState, study_profile: str) -> dict[str, Any]:
    """
    Build the YAML payload copied into a run snapshot.

    ``workspace`` uses the workspace ``project.yaml``. ``ideal_1mw`` uses the bundled
    ``config/project.ideal_1mw.yaml`` (simulation/sensitivity/plant name from that file) while
    **input CSV paths** are still normalized to the workspace ``inputs/*.csv`` used for the run.
    """
    profile = normalize_study_profile(study_profile)
    if profile == STUDY_PROFILE_IDEAL_1MW:
        base = _load_yaml_mapping(resolve_ideal_preset_path())
    else:
        base = load_project_payload(state)
    return _normalize_workspace_payload(deepcopy(base))


def project_config_for_study_profile_preview(state: WorkspaceState, study_profile: str) -> ProjectConfig:
    """Load the same project model used for run snapshots (workspace YAML or ideal preset + workspace inputs)."""
    payload = study_config_payload_for_snapshot(state, study_profile)
    preview_path = state.config_dir / ".form_preview.tmp.yaml"
    _write_validated_config(preview_path, payload)
    try:
        return ProjectConfig.from_yaml(preview_path)
    finally:
        if preview_path.exists():
            preview_path.unlink(missing_ok=True)


def config_form_api_values(project: ProjectConfig) -> dict[str, Any]:
    """Flat field map matching the config page form (for JSON + client-side preview)."""
    proj = project.project
    sim = project.simulation
    prep = sim.preprocessing
    grid = sim.grid
    load = sim.load
    bat = sim.battery
    sen = project.sensitivity

    def loss_table_text(table: dict[float, float]) -> str:
        lines = [f"{key}: {value}" for key, value in table.items()]
        return "\n".join(lines) + ("\n" if lines else "")

    def join_floats(values: list[float]) -> str:
        return ", ".join(str(v) for v in values)

    return {
        "project.plant_name": proj.plant_name,
        "project.simulation_start": proj.simulation_start.strftime("%Y-%m-%d %H:%M"),
        "project.simulation_end": proj.simulation_end.strftime("%Y-%m-%d %H:%M"),
        "simulation.data.solar_enabled": sim.data.solar_enabled,
        "simulation.data.wind_enabled": sim.data.wind_enabled,
        "simulation.preprocessing.frequency": prep.frequency,
        "simulation.preprocessing.gap_fill": prep.gap_fill,
        "simulation.preprocessing.max_interpolation_gap_minutes": prep.max_interpolation_gap_minutes,
        "simulation.preprocessing.align_to_full_year": prep.align_to_full_year,
        "simulation.preprocessing.simulation_dtype": prep.simulation_dtype,
        "simulation.grid.export_limit_kw": grid.export_limit_kw,
        "simulation.grid.import_limit_kw": "" if grid.import_limit_kw is None else grid.import_limit_kw,
        "simulation.load.profile_mode": load.profile_mode,
        "simulation.load.profile_template_id": load.profile_template_id or "",
        "simulation.load.contracted_capacity_mw": ""
        if load.contracted_capacity_mw is None
        else load.contracted_capacity_mw,
        "simulation.load.output_profile_kw": "" if load.output_profile_kw is None else load.output_profile_kw,
        "simulation.load.aux_consumption_kw": load.aux_consumption_kw,
        "simulation.battery.nominal_power_kw": bat.nominal_power_kw,
        "simulation.battery.duration_hours": bat.duration_hours,
        "simulation.battery.charge_efficiency": bat.charge_efficiency,
        "simulation.battery.discharge_efficiency": bat.discharge_efficiency,
        "simulation.battery.degradation_per_cycle": bat.degradation_per_cycle,
        "simulation.battery.initial_soc_fraction": bat.initial_soc_fraction,
        "simulation.battery.min_soc_fraction": bat.min_soc_fraction,
        "simulation.battery.max_soc_fraction": bat.max_soc_fraction,
        "simulation.battery.charge_loss_table": loss_table_text(bat.charge_loss_table),
        "simulation.battery.discharge_loss_table": loss_table_text(bat.discharge_loss_table),
        "sensitivity.wind_multipliers": join_floats(sen.wind_multipliers),
        "sensitivity.solar_multipliers": join_floats(sen.solar_multipliers),
        "sensitivity.profile_multipliers": join_floats(sen.profile_multipliers),
        "sensitivity.battery_capacity_kwh_values": join_floats(sen.battery_capacity_kwh_values),
        "sensitivity.battery_duration_hour_values": join_floats(sen.battery_duration_hour_values),
    }


def ensure_workspace_ready(
    workspace_root: str | Path | None = None,
    *,
    source_config_path: str | Path | None = None,
) -> WorkspaceState:
    """Create or bootstrap the local workspace used by the control room."""
    source_path = _resolve_source_config_path(source_config_path)
    root = _resolve_workspace_root(workspace_root)
    state = WorkspaceState(
        root=root,
        config_dir=root / "config",
        config_path=root / "config" / "project.yaml",
        inputs_dir=root / "inputs",
        runs_dir=root / "runs",
        metadata_path=root / "inputs" / "metadata.json",
        source_config_path=source_path,
    )
    state.config_dir.mkdir(parents=True, exist_ok=True)
    state.inputs_dir.mkdir(parents=True, exist_ok=True)
    state.runs_dir.mkdir(parents=True, exist_ok=True)

    if not state.config_path.exists():
        source_payload = _load_yaml_mapping(source_path)
        _write_yaml(state.config_path, _normalize_workspace_payload(source_payload))
    else:
        existing_payload = _load_yaml_mapping(state.config_path)
        _write_yaml(state.config_path, _normalize_workspace_payload(existing_payload))

    source_config = ProjectConfig.from_yaml(source_path)
    source_inputs = {
        "solar": source_config.inputs.solar_path,
        "wind": source_config.inputs.wind_path,
        "output_profile": source_config.inputs.output_profile_path,
        "output_profile_18_22": source_config.inputs.output_profile_18_22_path,
        "aux_power": source_config.inputs.aux_power_path,
    }
    if any(spec.generated and not source_inputs[spec.key].exists() for spec in MANAGED_INPUT_SPECS):
        generate_tender_input_files(source_config)
    metadata = _load_metadata(state)
    for spec in MANAGED_INPUT_SPECS:
        target_path = state.inputs_dir / spec.canonical_name
        if not target_path.exists():
            shutil.copy2(source_inputs[spec.key], target_path)
            metadata.setdefault(spec.key, {})
            metadata[spec.key].update(
                {
                    "original_name": source_inputs[spec.key].name,
                    "source": "seed",
                    "updated_at": _iso_now(),
                }
            )
    _save_metadata(state, metadata)
    return state


def load_project_config(state: WorkspaceState) -> ProjectConfig:
    return ProjectConfig.from_yaml(state.config_path)


def load_project_payload(state: WorkspaceState) -> dict[str, Any]:
    return _load_yaml_mapping(state.config_path)


def save_project_form(state: WorkspaceState, form_data: dict[str, str]) -> ProjectConfig:
    """Persist the form-backed project configuration."""
    payload = load_project_payload(state)
    project_payload = deepcopy(payload)
    for key, raw_value in form_data.items():
        if key in FORM_SKIP_KEYS:
            continue
        if key in BOOL_KEYS:
            value: Any = str(raw_value).lower() in {"true", "on", "1", "yes"}
        elif key in TABLE_KEYS:
            parsed = yaml.safe_load((raw_value or "").strip() or "{}")
            if not isinstance(parsed, dict):
                raise ValueError(f"{key} must be a mapping of C-rate to loss.")
            value = {float(k): float(v) for k, v in parsed.items()}
        elif key in LIST_FLOAT_KEYS:
            text = (raw_value or "").replace(",", " ")
            value = [float(part) for part in text.split() if part.strip()]
        elif key in INT_KEYS:
            value = int(raw_value)
        elif key in NULLABLE_FLOAT_KEYS:
            text = (raw_value or "").strip()
            value = None if text in {"", "null", "None", "none"} else float(text)
        elif key in FLOAT_KEYS:
            value = float(raw_value)
        elif key in TEXT_KEYS:
            value = raw_value.strip() if isinstance(raw_value, str) else raw_value
            if key == "simulation.load.profile_template_id" and value == "":
                value = None
        else:
            continue
        _set_nested(project_payload, key, value)

    # Checkboxes are absent when false.
    for key in BOOL_KEYS:
        if key not in form_data:
            _set_nested(project_payload, key, False)

    normalized = _normalize_workspace_payload(project_payload)
    _write_validated_config(state.config_path, normalized)
    return ProjectConfig.from_yaml(state.config_path)


def list_managed_inputs(state: WorkspaceState) -> list[ManagedInput]:
    metadata = _load_metadata(state)
    inputs: list[ManagedInput] = []
    for spec in MANAGED_INPUT_SPECS:
        path = state.inputs_dir / spec.canonical_name
        exists = path.exists()
        validation_ok = False
        validation_message = "Missing file."
        size_kb = None
        modified_at = None
        if exists:
            validation_ok, validation_message = validate_input_file(spec.key, path)
            stat = path.stat()
            size_kb = stat.st_size / 1024.0
            modified_at = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        meta = metadata.get(spec.key, {})
        inputs.append(
            ManagedInput(
                key=spec.key,
                label=spec.label,
                canonical_name=spec.canonical_name,
                absolute_path=path,
                expected_headers=spec.expected_headers,
                description=spec.description,
                exists=exists,
                original_name=meta.get("original_name"),
                source=meta.get("source"),
                modified_at=modified_at,
                size_kb=size_kb,
                validation_ok=validation_ok,
                validation_message=validation_message,
            )
        )
    return inputs


def store_uploaded_input(state: WorkspaceState, input_key: str, upload: Any) -> ManagedInput:
    spec = _require_input_spec(input_key)
    if upload is None or not getattr(upload, "filename", ""):
        raise ValueError("Choose a CSV file to upload.")
    filename = str(upload.filename)
    if not filename.lower().endswith(".csv"):
        raise ValueError("Only CSV uploads are supported.")

    payload = upload.read()
    if not payload:
        raise ValueError("Uploaded file is empty.")
    _validate_csv_bytes(spec, payload)

    target_path = state.inputs_dir / spec.canonical_name
    target_path.write_bytes(payload)

    metadata = _load_metadata(state)
    metadata.setdefault(input_key, {})
    metadata[input_key].update(
        {
            "original_name": filename,
            "source": "upload",
            "updated_at": _iso_now(),
        }
    )
    _save_metadata(state, metadata)
    return next(item for item in list_managed_inputs(state) if item.key == input_key)


def resolve_ideal_preset_path() -> Path:
    """Locate the bundled `project.ideal_1mw.yaml` (repo or PyInstaller tree)."""
    for base in (bundled_root(), repo_root()):
        candidate = (base / "config" / "project.ideal_1mw.yaml").resolve()
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("project.ideal_1mw.yaml not found under config/ (repo or bundle).")


def apply_ideal_study_preset(state: WorkspaceState) -> ProjectConfig:
    """Merge simulation + sensitivity from the ideal example into the workspace project.yaml."""
    ideal = _load_yaml_mapping(resolve_ideal_preset_path())
    current = load_project_payload(state)
    merged = deepcopy(current)
    merged["simulation"] = deepcopy(ideal.get("simulation", merged.get("simulation", {})))
    merged["sensitivity"] = deepcopy(ideal.get("sensitivity", merged.get("sensitivity", {})))
    _write_validated_config(state.config_path, merged)
    return ProjectConfig.from_yaml(state.config_path)


def ideal_tile_generation_profiles(
    state: WorkspaceState,
    *,
    solar_scale: float = 1.0,
    wind_scale: float = 1.0,
) -> dict[str, Any]:
    """
    Overwrite workspace `solar.csv` / `wind.csv` by tiling the current files across the simulation horizon.

    Uses dates from the workspace config. Intended after uploading short seed series (e.g. one day).
    """
    project = load_project_config(state)
    solar_path = state.inputs_dir / "solar.csv"
    wind_path = state.inputs_dir / "wind.csv"
    if not solar_path.is_file() or not wind_path.is_file():
        raise FileNotFoundError("Workspace solar.csv and wind.csv must exist before tiling.")
    write_tiled_year_profiles(
        simulation_start=project.project.simulation_start,
        simulation_end=project.project.simulation_end,
        solar_source=solar_path,
        wind_source=wind_path,
        solar_out=solar_path,
        wind_out=wind_path,
        solar_scale=float(solar_scale),
        wind_scale=float(wind_scale),
    )
    metadata = _load_metadata(state)
    for key in ("solar", "wind"):
        metadata.setdefault(key, {})
        metadata[key].update(
            {
                "original_name": f"ideal_tile:{key}.csv",
                "source": "ideal_tile",
                "updated_at": _iso_now(),
            }
        )
    _save_metadata(state, metadata)
    return {
        "solar_path": str(solar_path),
        "wind_path": str(wind_path),
        "simulation_start": project.project.simulation_start.isoformat(sep=" "),
        "simulation_end": project.project.simulation_end.isoformat(sep=" "),
        "solar_scale": float(solar_scale),
        "wind_scale": float(wind_scale),
    }


def aligned_energy_report_payload(state: WorkspaceState, *, excess_fraction: float = 0.08) -> dict[str, Any]:
    """Summary plus heuristic scale suggestions (annual kWh) for balancing RE vs load."""
    project = load_project_config(state)
    summary = summarize_aligned_inputs(project.simulation)
    suggestions = suggest_alignment_scales(
        summary,
        solar_multiplier=project.simulation.data.solar_multiplier,
        wind_multiplier=project.simulation.data.wind_multiplier,
        profile_multiplier=project.simulation.load.profile_multiplier,
        excess_fraction=float(excess_fraction),
    )
    return {"summary": asdict(summary), "suggestions": suggestions}


def apply_alignment_renewable_scales(state: WorkspaceState, *, excess_fraction: float = 0.08) -> dict[str, float]:
    """Multiply solar and wind multipliers by the suggested uniform renewable scale."""
    project = load_project_config(state)
    summary = summarize_aligned_inputs(project.simulation)
    sug = suggest_alignment_scales(
        summary,
        solar_multiplier=project.simulation.data.solar_multiplier,
        wind_multiplier=project.simulation.data.wind_multiplier,
        profile_multiplier=project.simulation.load.profile_multiplier,
        excess_fraction=float(excess_fraction),
    )
    k = float(sug["uniform_renewable_scale"])
    payload = load_project_payload(state)
    sim = payload.setdefault("simulation", {})
    data = sim.setdefault("data", {})
    data["solar_multiplier"] = float(data.get("solar_multiplier", 1.0)) * k
    data["wind_multiplier"] = float(data.get("wind_multiplier", 1.0)) * k
    _write_validated_config(state.config_path, payload)
    return {
        "applied_uniform_renewable_scale": k,
        "solar_multiplier": float(data["solar_multiplier"]),
        "wind_multiplier": float(data["wind_multiplier"]),
    }


def apply_alignment_profile_scale(state: WorkspaceState, *, excess_fraction: float = 0.08) -> dict[str, float]:
    """Multiply load profile_multiplier by the suggested factor (reduces output load when RE is weak)."""
    project = load_project_config(state)
    summary = summarize_aligned_inputs(project.simulation)
    sug = suggest_alignment_scales(
        summary,
        solar_multiplier=project.simulation.data.solar_multiplier,
        wind_multiplier=project.simulation.data.wind_multiplier,
        profile_multiplier=project.simulation.load.profile_multiplier,
        excess_fraction=float(excess_fraction),
    )
    k = float(sug["profile_multiplier_scale"])
    payload = load_project_payload(state)
    sim = payload.setdefault("simulation", {})
    load = sim.setdefault("load", {})
    new_pm = float(load.get("profile_multiplier", 1.0)) * k
    load["profile_multiplier"] = max(new_pm, 1e-6)
    _write_validated_config(state.config_path, payload)
    return {
        "applied_profile_multiplier_scale": k,
        "profile_multiplier": float(load["profile_multiplier"]),
    }


def generate_active_inputs(state: WorkspaceState) -> list[ManagedInput]:
    project = ProjectConfig.from_yaml(state.config_path)
    generated_paths = generate_tender_input_files(project)
    metadata = _load_metadata(state)
    path_to_key = {
        str(project.inputs.output_profile_path): "output_profile",
        str(project.inputs.output_profile_18_22_path): "output_profile_18_22",
        str(project.inputs.aux_power_path): "aux_power",
    }
    for path in generated_paths:
        key = path_to_key[str(path)]
        metadata.setdefault(key, {})
        metadata[key].update(
            {
                "original_name": f"generated:{path.name}",
                "source": "generated",
                "updated_at": _iso_now(),
            }
        )
    _save_metadata(state, metadata)
    return list_managed_inputs(state)


def validate_input_file(input_key: str, path: Path) -> tuple[bool, str]:
    spec = _require_input_spec(input_key)
    try:
        _validate_csv_headers(spec, path)
    except Exception as exc:  # pragma: no cover - exercised through caller
        return False, str(exc)
    return True, "Headers OK."


def create_run_snapshot(
    state: WorkspaceState,
    *,
    study_profile: str = STUDY_PROFILE_WORKSPACE,
) -> tuple[str, Path, Path, Path]:
    """Create an immutable run snapshot of the current inputs and selected study YAML profile."""
    payload = study_config_payload_for_snapshot(state, study_profile)
    run_id = _new_run_id()
    run_dir = state.runs_dir / run_id
    run_config_dir = run_dir / "config"
    run_inputs_dir = run_dir / "inputs"
    package_dir = run_dir / "package"
    run_config_dir.mkdir(parents=True, exist_ok=True)
    run_inputs_dir.mkdir(parents=True, exist_ok=True)
    package_dir.mkdir(parents=True, exist_ok=True)

    for spec in MANAGED_INPUT_SPECS:
        source_path = state.inputs_dir / spec.canonical_name
        if not source_path.exists():
            raise FileNotFoundError(f"Managed input missing: {spec.label}")
        shutil.copy2(source_path, run_inputs_dir / spec.canonical_name)

    snapshot_payload = deepcopy(payload)
    snapshot_payload["project"]["output_dir"] = "../package"
    snapshot_payload["inputs"] = {
        "solar_path": "../inputs/solar.csv",
        "wind_path": "../inputs/wind.csv",
        "output_profile_path": "../inputs/output_profile.csv",
        "output_profile_18_22_path": "../inputs/output_profile_18_22.csv",
        "aux_power_path": "../inputs/aux_power.csv",
    }
    config_path = run_config_dir / "project.yaml"
    _write_validated_config(config_path, snapshot_payload)
    _write_run_json(
        run_dir / "run.json",
        {
            "run_id": run_id,
            "status": "running",
            "plant_name": str(snapshot_payload["project"]["plant_name"]),
            "started_at": _iso_now(),
            "finished_at": None,
            "error": None,
            "summary_metrics": {},
            "inputs": _build_input_fingerprints(run_inputs_dir),
            "artifacts": [],
        },
    )
    return run_id, run_dir, config_path, package_dir


def run_study(
    state: WorkspaceState,
    *,
    progress_callback: ProgressCallback | None = None,
    dump_sections: bool = False,
    study_profile: str = STUDY_PROFILE_WORKSPACE,
) -> RunRecord:
    """Execute the full study in a fresh run directory and persist run metadata."""
    run_id, run_dir, config_path, package_dir = create_run_snapshot(state, study_profile=study_profile)
    return execute_run_snapshot(
        state,
        run_id=run_id,
        run_dir=run_dir,
        config_path=config_path,
        package_dir=package_dir,
        progress_callback=progress_callback,
        dump_sections=dump_sections,
    )


def execute_run_snapshot(
    state: WorkspaceState,
    *,
    run_id: str,
    run_dir: Path,
    config_path: Path,
    package_dir: Path,
    progress_callback: ProgressCallback | None = None,
    dump_sections: bool = False,
) -> RunRecord:
    """Execute a previously created immutable run snapshot."""
    run_json_path = run_dir / "run.json"
    try:
        project = ProjectConfig.from_yaml(config_path)
        result = run_full_study(
            project,
            dump_sections=dump_sections,
            package_dir=package_dir,
            progress_callback=progress_callback,
        )
        summary_metrics = _load_summary_metrics(result.package_dir)
        update_run_status(
            state,
            run_id,
            status="completed",
            finished_at=_iso_now(),
            summary_metrics=summary_metrics,
            artifacts=_serialize_artifacts(_build_artifact_index(result.package_dir)),
            error=None,
        )
    except StudyCancelledError as exc:
        update_run_status(
            state,
            run_id,
            status="cancelled",
            finished_at=_iso_now(),
            error=str(exc),
            artifacts=_serialize_artifacts(_build_artifact_index(package_dir)),
        )
        raise
    except Exception as exc:
        update_run_status(
            state,
            run_id,
            status="failed",
            finished_at=_iso_now(),
            error=str(exc),
            artifacts=_serialize_artifacts(_build_artifact_index(package_dir)),
        )
        raise
    return get_run_record(state, run_id)


def list_run_records(state: WorkspaceState) -> list[RunRecord]:
    records: list[RunRecord] = []
    for run_json_path in sorted(state.runs_dir.glob("*/run.json")):
        try:
            records.append(_run_record_from_json(run_json_path))
        except Exception:
            continue
    records.sort(key=lambda item: item.started_at, reverse=True)
    return records


def get_run_record(state: WorkspaceState, run_id: str) -> RunRecord:
    run_json_path = state.runs_dir / run_id / "run.json"
    if not run_json_path.exists():
        raise FileNotFoundError(f"Run not found: {run_id}")
    return _run_record_from_json(run_json_path)


def get_latest_run_record(state: WorkspaceState) -> RunRecord | None:
    runs = list_run_records(state)
    return runs[0] if runs else None


def update_run_status(
    state: WorkspaceState,
    run_id: str,
    *,
    status: str,
    finished_at: str | None = None,
    error: str | None = None,
    summary_metrics: dict[str, Any] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
) -> RunRecord:
    run_json_path = state.runs_dir / run_id / "run.json"
    metadata = _load_run_json(run_json_path)
    metadata["status"] = status
    metadata["finished_at"] = finished_at
    metadata["error"] = error
    if summary_metrics is not None:
        metadata["summary_metrics"] = summary_metrics
    if artifacts is not None:
        metadata["artifacts"] = artifacts
    _write_run_json(run_json_path, metadata)
    return _run_record_from_json(run_json_path)


def delete_run_record(state: WorkspaceState, run_id: str) -> None:
    run_dir = state.runs_dir / run_id
    run_json_path = run_dir / "run.json"
    if not run_json_path.exists():
        raise FileNotFoundError(f"Run not found: {run_id}")
    shutil.rmtree(run_dir)


def resolve_run_artifact(record: RunRecord, relative_path: str) -> Path:
    target = (record.package_dir / relative_path).resolve()
    if record.package_dir.resolve() not in target.parents and target != record.package_dir.resolve():
        raise ValueError("Artifact path is outside the package directory.")
    if not target.exists() or not target.is_file():
        raise FileNotFoundError(relative_path)
    return target


def chart_dataset_options(record: RunRecord) -> list[RunArtifactIndex]:
    preferred = {
        "base_case_minute_flows.parquet",
        "base_case_profile_compliance_monthly.csv",
        "cases_table.csv",
        "sensitivity_cross_table.csv",
    }
    options = [artifact for artifact in record.artifacts if artifact.relative_path in preferred]
    options.sort(key=lambda item: item.relative_path)
    return options


def default_preview_artifact(record: RunRecord) -> str | None:
    preferred = (
        "base_case_minute_flows.parquet",
        "base_case_summary.csv",
        "cases_table.csv",
        "sensitivity_cross_table.csv",
    )
    by_name = {artifact.relative_path: artifact for artifact in record.artifacts}
    for name in preferred:
        if name in by_name:
            return name
    tabular = [artifact.relative_path for artifact in record.artifacts if artifact.is_tabular]
    return tabular[0] if tabular else None


def _format_sensitivity_case_label(row: dict[str, Any]) -> str:
    cid = str(row.get("case_id", "?"))
    if cid == "base":
        return "base — reference configuration"
    w = row.get("wind_multiplier")
    s = row.get("solar_multiplier")
    p = row.get("profile_multiplier")
    c = row.get("battery_capacity_kwh")
    h = row.get("battery_duration_hours")
    return f"{cid} — W×{w} S×{s} P×{p} | BESS {c} kWh / {h} h"


def load_sensitivity_case_option_groups(record: RunRecord) -> list[dict[str, Any]]:
    """Grouped options for the dashboard case picker (named cases, then factorial cross table)."""
    seen: set[str] = set()
    groups: list[dict[str, Any]] = []

    def consume(rows: list[dict[str, Any]], group_name: str) -> None:
        opts: list[dict[str, str]] = []
        for raw in rows:
            cid = str(raw.get("case_id", "")).strip()
            if not cid or cid == "base" or cid in seen:
                continue
            seen.add(cid)
            opts.append({"value": cid, "label": _format_sensitivity_case_label(raw)})
        if opts:
            groups.append({"group": group_name, "options": opts})

    try:
        cases_path = resolve_run_artifact(record, "cases_table.csv")
        consume(pl.read_csv(cases_path).to_dicts(), "Named sensitivity cases")
    except FileNotFoundError:
        pass

    try:
        cross_path = resolve_run_artifact(record, "sensitivity_cross_table.csv")
        consume(pl.read_csv(cross_path).to_dicts(), "Cross product (full grid)")
    except FileNotFoundError:
        pass

    return groups


def resolve_dashboard_case_metrics(record: RunRecord, case_id: str | None) -> tuple[dict[str, Any], str]:
    """Return (summary row for metric cards, canonical case_id) — unknown ids fall back to base."""
    base = dict(record.summary_metrics or {})
    cid = (case_id or "").strip() or "base"
    if cid == "base":
        return base, "base"
    for path_name in ("cases_table.csv", "sensitivity_cross_table.csv"):
        try:
            path = resolve_run_artifact(record, path_name)
            for raw in pl.read_csv(path).to_dicts():
                if str(raw.get("case_id")) == cid:
                    return dict(raw), cid
        except FileNotFoundError:
            continue
    return base, "base"


def load_metric_cards(summary: dict[str, Any] | None) -> list[MetricCard]:
    row = summary or {}
    if not row:
        return []
    cards = [
        MetricCard("Rows", _format_number(row.get("rows"), digits=0), "Minute rows"),
        MetricCard(
            "Solar / wind (table basis)",
            f"{_format_number(row.get('solar_kw_min_sum'))} / {_format_number(row.get('wind_kw_min_sum'))} kW-min",
            "Same sums as Energy Table (aligned minute rows; sparse CSVs → small totals)",
        ),
        MetricCard(
            "RE non-zero minutes",
            f"{_format_number(row.get('solar_nonzero_minutes'), digits=0)} sol / {_format_number(row.get('wind_nonzero_minutes'), digits=0)} wind",
            f"Share of aligned horizon: {_format_number(row.get('solar_nonzero_fraction_pct'), digits=1)}% solar, "
            f"{_format_number(row.get('wind_nonzero_fraction_pct'), digits=1)}% wind",
        ),
        MetricCard("Grid Import", f"{_format_number(row.get('grid_import_kw_min'))} kW-min", "Energy from grid"),
        MetricCard("Grid Export", f"{_format_number(row.get('grid_export_kw_min'))} kW-min", "Energy sold to grid"),
        MetricCard("Self Consumption", f"{_format_number(row.get('self_consumption_pct'), digits=1)}%", "Renewables plus battery share"),
        MetricCard(
            "Battery Capacity",
            f"{_format_number(row.get('final_degraded_capacity_kw_min'))} kW-min",
            "Final degraded capacity",
        ),
        MetricCard("Final SOC", f"{_format_number(row.get('final_soc_pct'), digits=1)}%", "End-of-run state of charge"),
        MetricCard("Charge Count", _format_number(row.get("cumulative_charge_count"), digits=1), "Equivalent full cycles"),
        MetricCard("Identity 1", str(row.get("identity_1_failures", 0)), "Energy balance failures"),
        MetricCard("Identity 2", str(row.get("identity_2_failures", 0)), "BESS state failures"),
        MetricCard("Min Monthly DFR", f"{_format_number(row.get('min_monthly_dfr_pct'), digits=1)}%", "Worst monthly compliance"),
        MetricCard("Months Below DFR", _format_number(row.get("months_below_dfr_threshold"), digits=0), "Months below threshold"),
        MetricCard("Annual Gap", f"{_format_number(row.get('annual_energy_gap_kwh'))} kWh", "Annual profile gap"),
    ]
    return cards


def load_energy_table(record: RunRecord) -> list[EnergyTableRow]:
    path = resolve_run_artifact(record, "energy_table.csv")
    frame = pl.read_csv(path)
    if frame.height == 0:
        return []
    return [
        EnergyTableRow(
            category=str(row["category"]),
            element=str(row["element"]),
            value_kw_min=float(row["value_kw_min"]),
        )
        for row in frame.to_dicts()
    ]


def load_small_table(record: RunRecord, relative_path: str, limit: int = 12) -> list[dict[str, Any]]:
    path = resolve_run_artifact(record, relative_path)
    frame = _read_tabular_frame(path)
    return [_normalize_row(row) for row in frame.head(limit).to_dicts()]


def load_table_preview(record: RunRecord, relative_path: str, page: int, page_size: int) -> TablePreview:
    path = resolve_run_artifact(record, relative_path)
    safe_page = max(page, 1)
    safe_size = max(1, min(page_size, 100))
    if path.suffix.lower() == ".parquet":
        lazy = pl.scan_parquet(path)
    else:
        lazy = pl.scan_csv(path, try_parse_dates=True)
    total_rows = int(lazy.select(pl.len()).collect().item())
    total_pages = max(1, math.ceil(total_rows / safe_size)) if total_rows else 1
    safe_page = min(safe_page, total_pages)
    start = (safe_page - 1) * safe_size
    frame = lazy.slice(start, safe_size).collect()
    return TablePreview(
        columns=frame.columns,
        rows=[_normalize_row(row) for row in frame.to_dicts()],
        page=safe_page,
        page_size=safe_size,
        total_rows=total_rows,
        total_pages=total_pages,
    )


def build_dataset_chart_cards(
    record: RunRecord,
    dataset: str,
    *,
    svg_width: int = 1520,
    svg_height: int = 560,
) -> list[ChartCard]:
    path = resolve_run_artifact(record, dataset)
    frame = _read_tabular_frame(path)
    if frame.height == 0:
        return []

    def cc(
        df: pl.DataFrame,
        title: str,
        subtitle: str,
        columns: list[str],
        *,
        x_column: str = "timestamp",
    ) -> ChartCard:
        return _chart_card(
            df,
            title,
            subtitle,
            columns,
            x_column=x_column,
            svg_width=svg_width,
            svg_height=svg_height,
        )

    if path.name == "base_case_minute_flows.parquet":
        return [
            cc(frame, "Grid Import / Export", "Grid flows over the study horizon", ["grid_buy_kw", "grid_sell_kw"]),
            cc(frame, "Generation vs Consumption", "Total generation against total consumption", ["total_generation_kw", "total_consumption_kw"]),
            cc(frame, "Battery SOC", "State of charge (%)", ["soc_pct"]),
            cc(frame, "Battery Charge / Discharge", "Battery draw and store power", ["battery_draw_final_kw", "battery_store_final_kw"]),
        ]
    if path.name == "base_case_profile_compliance_monthly.csv":
        return [
            cc(
                frame,
                "Monthly DFR Compliance",
                "Monthly DFR against the required threshold",
                ["monthly_dfr_pct", "required_dfr_pct"],
                x_column="month_index",
            )
        ]
    if path.name == "cases_table.csv":
        charts: list[ChartCard] = []
        battery_cases = (
            frame.filter(pl.col("case_group").is_in(["base", "battery_capacity"]))
            .sort("battery_capacity_kwh")
            .unique(subset=["battery_capacity_kwh"], keep="first")
        )
        if battery_cases.height > 1:
            charts.append(
                cc(
                    battery_cases,
                    "Grid Import vs Capacity",
                    "Named-case grid import by battery capacity",
                    ["grid_import_kw_min"],
                    x_column="battery_capacity_kwh",
                )
            )
            charts.append(
                cc(
                    battery_cases,
                    "Self Consumption vs Capacity",
                    "Named-case self consumption by battery capacity",
                    ["self_consumption_pct"],
                    x_column="battery_capacity_kwh",
                )
            )
        profile_cases = (
            frame.filter(pl.col("case_group").is_in(["base", "profile"]))
            .sort("profile_multiplier")
            .unique(subset=["profile_multiplier"], keep="first")
        )
        if profile_cases.height > 1:
            charts.append(
                cc(
                    profile_cases,
                    "Annual Gap vs Profile Multiplier",
                    "Named-case annual energy gap by profile multiplier",
                    ["annual_energy_gap_kwh"],
                    x_column="profile_multiplier",
                )
            )
        return [chart for chart in charts if chart.svg]
    if path.name == "sensitivity_cross_table.csv":
        charts = []
        by_capacity = (
            frame.group_by("battery_capacity_kwh")
            .agg(
                pl.col("grid_import_kw_min").mean().alias("grid_import_kw_min"),
                pl.col("self_consumption_pct").mean().alias("self_consumption_pct"),
            )
            .sort("battery_capacity_kwh")
        )
        if by_capacity.height > 1:
            charts.append(
                cc(
                    by_capacity,
                    "Cross Grid Import vs Capacity",
                    "Average cross-case grid import by battery capacity",
                    ["grid_import_kw_min"],
                    x_column="battery_capacity_kwh",
                )
            )
            charts.append(
                cc(
                    by_capacity,
                    "Cross Self Consumption vs Capacity",
                    "Average cross-case self consumption by battery capacity",
                    ["self_consumption_pct"],
                    x_column="battery_capacity_kwh",
                )
            )
        by_profile = (
            frame.group_by("profile_multiplier")
            .agg(pl.col("annual_energy_gap_kwh").mean().alias("annual_energy_gap_kwh"))
            .sort("profile_multiplier")
        )
        if by_profile.height > 1:
            charts.append(
                cc(
                    by_profile,
                    "Cross Annual Gap vs Profile Multiplier",
                    "Average cross-case annual gap by profile multiplier",
                    ["annual_energy_gap_kwh"],
                    x_column="profile_multiplier",
                )
            )
        return [chart for chart in charts if chart.svg]
    return [
        ChartCard(
            "Preview Chart",
            "Auto-selected numeric columns",
            build_chart_svg_from_df(frame, None, "timestamp", width=svg_width, height=svg_height) or "",
        )
    ]


def build_chart_svg_from_df(
    df: pl.DataFrame,
    preferred_columns: list[str] | None = None,
    x_column: str = "timestamp",
    *,
    width: int = 1520,
    height: int = 560,
) -> str | None:
    if df.height == 0:
        return None
    numeric_columns = [
        column
        for column, dtype in zip(df.columns, df.dtypes, strict=True)
        if column != x_column and dtype.is_numeric()
    ]
    if not numeric_columns:
        return None
    columns = [column for column in (preferred_columns or []) if column in numeric_columns] or numeric_columns[:4]
    if not columns:
        return None

    max_points = max(400, min(1600, int(height * 2.4)))
    if df.height > max_points:
        step = max(1, math.ceil(df.height / max_points))
        df = df.gather_every(step)

    scale = max(1.0, min(2.05, height / 560.0, width / 1520.0))
    tick_fs = max(15, min(30, int(round(16 * scale))))
    axis_fs = max(16, min(32, int(round(18 * scale))))
    legend_fs = max(15, min(30, int(round(17 * scale))))
    tt_fs0 = max(13, int(round(14 * scale)))
    tt_fs1 = max(14, int(round(16 * scale)))
    line_stroke = max(2.6, min(5.8, 3.0 * scale))
    axis_stroke = max(1.2, min(2.2, 1.5 * scale))
    grid_stroke = max(0.9, min(1.4, 1.0 * scale))
    hover_r = max(10, int(round(11 * scale)))

    left_padding = max(76, min(132, int(0.068 * width)))
    right_padding = max(20, int(0.018 * width))
    top_padding = max(48, int(0.12 * height))
    bottom_padding = max(56, int(0.13 * height))
    chart_height = height - top_padding - bottom_padding
    chart_width = width - left_padding - right_padding
    y_axis_label = _infer_y_axis_label(columns)
    x_axis_label = _infer_x_axis_label(x_column)
    x_values, x_tick_values, x_tick_labels = _build_x_axis_scale(df, x_column)
    original_x_values = df[x_column].to_list() if x_column in df.columns else [str(int(v)) for v in x_values]
    x_min = min(x_values)
    x_max = max(x_values)
    x_span = max(x_max - x_min, 1.0)
    max_value = max(float(df.select(pl.max_horizontal([pl.col(column) for column in columns]).max()).item()), 1.0)
    colors = ["#0f766e", "#ea580c", "#2563eb", "#7c3aed", "#dc2626"]

    legend_stride = max(150, int(0.11 * width))
    legend_box = max(11, int(round(12 * scale)))
    legend_y = max(12, int(0.028 * height))
    legend_text_dy = int(legend_box * 0.78) + 2
    tw = min(300, max(220, int(0.19 * width)))
    th = max(50, int(0.095 * height))
    y_label_x = max(22, int(left_padding * 0.34))
    y_mid = top_padding + chart_height / 2
    x_tick_len = max(5, int(6 * scale))
    x_tick_text_dy = max(18, int(22 * scale))

    series_svg: list[str] = []
    hover_svg: list[str] = []
    for color_index, column in enumerate(columns):
        values = df[column].cast(pl.Float64).to_list()
        color = colors[color_index % len(colors)]
        points: list[str] = []
        for index, value in enumerate(values):
            if value is None:
                continue
            x = left_padding + ((x_values[index] - x_min) / x_span) * chart_width
            y = height - bottom_padding - ((float(value) / max_value) * chart_height)
            points.append(f"{x:.2f},{y:.2f}")
            half_w = tw / 2
            tooltip_x = max(min(x, width - half_w - 12), half_w + 12)
            tooltip_y = max(y - th * 0.55, top_padding + th * 0.6)
            hover_svg.append(
                f'<g class="chart-point-group">'
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{hover_r}" fill="rgba(0,0,0,0.001)" class="chart-point-hover" pointer-events="all" />'
                f'<g class="chart-tooltip-group">'
                f'<rect x="{tooltip_x - half_w:.2f}" y="{tooltip_y - th:.2f}" width="{tw:.2f}" height="{th:.2f}" fill="#0f172a" fill-opacity="0.94" rx="6" />'
                f'<text x="{tooltip_x:.2f}" y="{tooltip_y - th * 0.58:.2f}" text-anchor="middle" fill="#94a3b8" font-size="{tt_fs0}">{html.escape(str(original_x_values[index]))}</text>'
                f'<text x="{tooltip_x:.2f}" y="{tooltip_y - th * 0.22:.2f}" text-anchor="middle" fill="#f8fafc" font-size="{tt_fs1}" font-weight="600">{html.escape(column)}: {_format_number(value)}</text>'
                f"</g>"
                f"</g>"
            )
        legend_x = left_padding + (color_index * legend_stride)
        series_svg.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="{line_stroke:.2f}" points="{" ".join(points)}" />'
        )
        series_svg.append(
            f'<rect x="{legend_x}" y="{legend_y}" width="{legend_box}" height="{legend_box}" rx="2" fill="{color}" />'
            f'<text x="{legend_x + legend_box + 8}" y="{legend_y + legend_text_dy}" fill="#334155" font-size="{legend_fs}" font-weight="600">{html.escape(column)}</text>'
        )

    ticks: list[str] = []
    tick_pad = max(10, int(12 * scale))
    for tick_value in _build_y_ticks(max_value):
        y = height - bottom_padding - ((tick_value / max_value) * chart_height)
        ticks.append(
            f'<line x1="{left_padding}" y1="{y:.2f}" x2="{width - right_padding}" y2="{y:.2f}" stroke="#e2e8f0" stroke-width="{grid_stroke:.2f}" />'
        )
        ticks.append(
            f'<text x="{left_padding - tick_pad}" y="{y + tick_fs * 0.32:.2f}" text-anchor="end" fill="#64748b" font-size="{tick_fs}">{html.escape(_format_tick_value(tick_value))}</text>'
        )
    for tick_value, tick_label in zip(x_tick_values, x_tick_labels, strict=True):
        x = left_padding + ((tick_value - x_min) / x_span) * chart_width
        ticks.append(
            f'<line x1="{x:.2f}" y1="{height - bottom_padding}" x2="{x:.2f}" y2="{height - bottom_padding + x_tick_len}" stroke="#cbd5e1" stroke-width="{grid_stroke:.2f}" />'
        )
        ticks.append(
            f'<text x="{x:.2f}" y="{height - bottom_padding + x_tick_text_dy}" text-anchor="middle" fill="#64748b" font-size="{tick_fs}">{html.escape(tick_label)}</text>'
        )

    x_axis_title_y = height - max(10, int(height * 0.022))
    return (
        f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="xMidYMid meet" class="chart-svg">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" rx="16" />'
        f'<line x1="{left_padding}" y1="{height - bottom_padding}" x2="{width - right_padding}" y2="{height - bottom_padding}" stroke="#cbd5e1" stroke-width="{axis_stroke:.2f}" />'
        f'<line x1="{left_padding}" y1="{top_padding}" x2="{left_padding}" y2="{height - bottom_padding}" stroke="#cbd5e1" stroke-width="{axis_stroke:.2f}" />'
        f'{"".join(ticks)}'
        f'<text x="{left_padding + chart_width / 2:.2f}" y="{x_axis_title_y}" text-anchor="middle" fill="#475569" font-weight="600" font-size="{axis_fs}">{html.escape(x_axis_label)}</text>'
        f'<text x="{y_label_x}" y="{y_mid:.2f}" text-anchor="middle" fill="#475569" font-weight="600" font-size="{axis_fs}" transform="rotate(-90 {y_label_x} {y_mid:.2f})">{html.escape(y_axis_label)}</text>'
        f'{"".join(series_svg)}'
        f'{"".join(hover_svg)}'
        "</svg>"
    )


def dataset_label(relative_path: str) -> str:
    labels = {
        "base_case_minute_flows.parquet": "Base Minute Flows",
        "base_case_profile_compliance_monthly.csv": "Monthly Compliance",
        "cases_table.csv": "Cases Table",
        "sensitivity_cross_table.csv": "Sensitivity Cross",
    }
    return labels.get(relative_path, relative_path)


def artifact_label(relative_path: str) -> str:
    return relative_path.replace("_", " ").replace(".csv", "").replace(".parquet", "").title()


def _chart_card(
    df: pl.DataFrame,
    title: str,
    subtitle: str,
    columns: list[str],
    *,
    x_column: str = "timestamp",
    svg_width: int = 1520,
    svg_height: int = 560,
) -> ChartCard:
    return ChartCard(
        title=title,
        subtitle=subtitle,
        svg=build_chart_svg_from_df(df, columns, x_column, width=svg_width, height=svg_height) or "",
    )


def _read_tabular_frame(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path, try_parse_dates=True)


def _load_summary_metrics(package_dir: Path) -> dict[str, Any]:
    path = package_dir / "base_summary.csv"
    if not path.exists():
        return {}
    frame = pl.read_csv(path)
    return frame.to_dicts()[0] if frame.height else {}


def _build_artifact_index(package_dir: Path) -> list[RunArtifactIndex]:
    artifacts: list[RunArtifactIndex] = []
    for path in sorted(package_dir.rglob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        artifacts.append(
            RunArtifactIndex(
                relative_path=str(path.relative_to(package_dir)),
                absolute_path=path,
                size_kb=stat.st_size / 1024.0,
                modified_at=datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                is_tabular=path.suffix.lower() in {".csv", ".parquet"},
            )
        )
    return artifacts


def _serialize_artifacts(artifacts: list[RunArtifactIndex]) -> list[dict[str, Any]]:
    return [
        {
            "relative_path": artifact.relative_path,
            "size_kb": artifact.size_kb,
            "modified_at": artifact.modified_at,
            "is_tabular": artifact.is_tabular,
        }
        for artifact in artifacts
    ]


def _build_input_fingerprints(inputs_dir: Path) -> list[dict[str, Any]]:
    fingerprints: list[dict[str, Any]] = []
    for spec in MANAGED_INPUT_SPECS:
        path = inputs_dir / spec.canonical_name
        fingerprints.append(
            {
                "key": spec.key,
                "canonical_name": spec.canonical_name,
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return fingerprints


def _run_record_from_json(path: Path) -> RunRecord:
    payload = _load_run_json(path)
    run_dir = path.parent
    package_dir = run_dir / "package"
    artifacts = [
        RunArtifactIndex(
            relative_path=str(item["relative_path"]),
            absolute_path=package_dir / str(item["relative_path"]),
            size_kb=float(item["size_kb"]),
            modified_at=str(item["modified_at"]),
            is_tabular=bool(item["is_tabular"]),
        )
        for item in payload.get("artifacts", [])
    ]
    return RunRecord(
        run_id=str(payload["run_id"]),
        run_dir=run_dir,
        package_dir=package_dir,
        config_path=run_dir / "config" / "project.yaml",
        status=str(payload.get("status", "unknown")),
        plant_name=str(payload.get("plant_name", "")),
        started_at=str(payload.get("started_at", "")),
        finished_at=payload.get("finished_at"),
        artifacts=artifacts,
        summary_metrics=dict(payload.get("summary_metrics", {})),
        error=payload.get("error"),
    )


def _load_metadata(state: WorkspaceState) -> dict[str, dict[str, Any]]:
    if not state.metadata_path.exists():
        return {}
    payload = json.loads(state.metadata_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _save_metadata(state: WorkspaceState, payload: dict[str, dict[str, Any]]) -> None:
    state.metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_run_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _write_run_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _resolve_workspace_root(workspace_root: str | Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root).expanduser().resolve()
    env_root = os.environ.get("SECI_FDRE_V_WORKSPACE")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return (_repo_root() / ".workspace").resolve()


def _resolve_source_config_path(path: str | Path | None) -> Path:
    if path is not None:
        return Path(path).expanduser().resolve()
    env_path = os.environ.get("SECI_FDRE_V_SOURCE_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()

    candidates = [
        Path.cwd() / "config" / "project.yaml",
        resolve_seed_source_config_path(),
        repo_root() / "config" / "project.yaml",
    ]
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.exists():
            return resolved
    return candidates[0].expanduser().resolve()


def _repo_root() -> Path:
    return repo_root()


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Configuration must be a mapping.")
    return payload


def _normalize_workspace_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(payload)
    normalized.setdefault("project", {})
    normalized["project"].setdefault("plant_name", "seci_fdre_v_sample")
    normalized["project"]["output_dir"] = "../runs"
    normalized.setdefault("inputs", {})
    normalized["inputs"].update(
        {
            "solar_path": "../inputs/solar.csv",
            "wind_path": "../inputs/wind.csv",
            "output_profile_path": "../inputs/output_profile.csv",
            "output_profile_18_22_path": "../inputs/output_profile_18_22.csv",
            "aux_power_path": "../inputs/aux_power.csv",
        }
    )
    return normalized


def _write_validated_config(path: Path, payload: dict[str, Any]) -> None:
    normalized = _normalize_workspace_payload(payload)
    serialized = yaml.dump(normalized, sort_keys=False, default_flow_style=False)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as handle:
        handle.write(serialized)
        temp_path = Path(handle.name)
    try:
        ProjectConfig.from_yaml(temp_path)
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.dump(payload, sort_keys=False, default_flow_style=False), encoding="utf-8")


def _set_nested(payload: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    current: dict[str, Any] = payload
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _require_input_spec(input_key: str) -> ManagedInputSpec:
    if input_key not in SPEC_BY_KEY:
        raise KeyError(f"Unknown input key: {input_key}")
    return SPEC_BY_KEY[input_key]


def _validate_csv_bytes(spec: ManagedInputSpec, payload: bytes) -> None:
    reader = csv.reader(io.StringIO(payload.decode("utf-8-sig")))
    headers = next(reader, None)
    if headers is None:
        raise ValueError("CSV file is empty.")
    missing = [header for header in spec.expected_headers if header not in headers]
    if missing:
        raise ValueError(f"{spec.label} is missing required columns: {', '.join(missing)}")


def _validate_csv_headers(spec: ManagedInputSpec, path: Path) -> None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        headers = next(csv.reader(handle), None)
    if headers is None:
        raise ValueError("CSV file is empty.")
    missing = [header for header in spec.expected_headers if header not in headers]
    if missing:
        raise ValueError(f"{spec.label} is missing required columns: {', '.join(missing)}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _new_run_id() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S") + f"-{secrets.token_hex(3)}"


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _format_cell_value(value) for key, value in row.items()}


def _format_cell_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, float):
        return round(value, 6)
    return value


def _format_number(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if digits == 0:
        return f"{number:,.0f}"
    return f"{number:,.{digits}f}"


def _infer_x_axis_label(x_column: str) -> str:
    if x_column == "timestamp":
        return "Time"
    if x_column.endswith("_pct"):
        return f"{_humanize(x_column.removesuffix('_pct'))} (%)"
    if x_column.endswith("_kwh"):
        return f"{_humanize(x_column.removesuffix('_kwh'))} (kWh)"
    if x_column.endswith("_kw_min"):
        return f"{_humanize(x_column.removesuffix('_kw_min'))} (kW-min)"
    if x_column.endswith("_kw"):
        return f"{_humanize(x_column.removesuffix('_kw'))} (kW)"
    return _humanize(x_column)


def _infer_y_axis_label(columns: list[str]) -> str:
    if len(columns) == 1:
        column = columns[0]
        if column.endswith("_pct"):
            return "Percent (%)"
        if column.endswith("_kwh"):
            return "Energy (kWh)"
        if column.endswith("_kw_min"):
            return "Energy (kW-min)"
        if column.endswith("_kw"):
            return "Power (kW)"
    return "Value"


def _humanize(value: str) -> str:
    return " ".join("SOC" if part.lower() == "soc" else part.capitalize() for part in value.split("_"))


def _build_x_axis_scale(df: pl.DataFrame, x_column: str) -> tuple[list[float], list[float], list[str]]:
    if x_column in df.columns:
        series = df[x_column]
        dtype = df.schema[x_column]
        if dtype.is_temporal():
            datetimes = [value for value in series.to_list() if value is not None]
            if datetimes:
                numeric_values = [value.timestamp() for value in datetimes]
                tick_indices = _select_tick_indices(len(datetimes))
                tick_values = [numeric_values[index] for index in tick_indices]
                tick_labels = [_format_time_tick(datetimes[index], datetimes[0], datetimes[-1]) for index in tick_indices]
                return numeric_values, tick_values, tick_labels
        if dtype.is_numeric():
            numeric_values = [float(value) for value in series.cast(pl.Float64).to_list()]
            tick_indices = _select_tick_indices(len(numeric_values))
            tick_values = [numeric_values[index] for index in tick_indices]
            tick_labels = [_format_tick_value(numeric_values[index]) for index in tick_indices]
            return numeric_values, tick_values, tick_labels
    numeric_values = [float(index) for index in range(max(df.height, 1))]
    tick_indices = _select_tick_indices(len(numeric_values))
    tick_values = [numeric_values[index] for index in tick_indices]
    tick_labels = [str(index + 1) for index in tick_indices]
    return numeric_values, tick_values, tick_labels


def _select_tick_indices(length: int, tick_count: int = 4) -> list[int]:
    if length <= 1:
        return [0]
    if length <= tick_count:
        return list(range(length))
    indices = [round(step * (length - 1) / (tick_count - 1)) for step in range(tick_count)]
    deduped: list[int] = []
    for index in indices:
        if index not in deduped:
            deduped.append(index)
    return deduped


def _format_time_tick(value: datetime, start: datetime, end: datetime) -> str:
    if start.date() == end.date():
        return value.strftime("%H:%M")
    if start.year == end.year:
        return value.strftime("%d %b")
    return value.strftime("%Y-%m")


def _build_y_ticks(max_value: float, tick_count: int = 4) -> list[float]:
    if max_value <= 0:
        return [0.0]
    return [max_value * step / tick_count for step in range(tick_count + 1)]


def _format_tick_value(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    if value.is_integer():
        return f"{value:.0f}"
    return f"{value:.1f}"
