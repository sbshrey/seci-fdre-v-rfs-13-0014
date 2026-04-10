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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import polars as pl
import yaml

from seci_fdre_v_model.config import ProjectConfig
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


def create_run_snapshot(state: WorkspaceState) -> tuple[str, Path, Path, Path]:
    """Create an immutable run snapshot of the current config and inputs."""
    payload = load_project_payload(state)
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
) -> RunRecord:
    """Execute the full study in a fresh run directory and persist run metadata."""
    run_id, run_dir, config_path, package_dir = create_run_snapshot(state)
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
        metadata = _load_run_json(run_json_path)
        metadata.update(
            {
                "status": "completed",
                "finished_at": _iso_now(),
                "summary_metrics": summary_metrics,
                "artifacts": _serialize_artifacts(_build_artifact_index(result.package_dir)),
            }
        )
        _write_run_json(run_json_path, metadata)
    except Exception as exc:
        metadata = _load_run_json(run_json_path)
        metadata.update(
            {
                "status": "failed",
                "finished_at": _iso_now(),
                "error": str(exc),
            }
        )
        _write_run_json(run_json_path, metadata)
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


def load_metric_cards(record: RunRecord) -> list[MetricCard]:
    row = record.summary_metrics
    if not row:
        return []
    cards = [
        MetricCard("Rows", _format_number(row.get("rows"), digits=0), "Minute rows"),
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


def build_dataset_chart_cards(record: RunRecord, dataset: str) -> list[ChartCard]:
    path = resolve_run_artifact(record, dataset)
    frame = _read_tabular_frame(path)
    if frame.height == 0:
        return []

    if path.name == "base_case_minute_flows.parquet":
        return [
            _chart_card(frame, "Grid Import / Export", "Grid flows over the study horizon", ["grid_buy_kw", "grid_sell_kw"]),
            _chart_card(frame, "Generation vs Consumption", "Total generation against total consumption", ["total_generation_kw", "total_consumption_kw"]),
            _chart_card(frame, "Battery SOC", "State of charge (%)", ["soc_pct"]),
            _chart_card(frame, "Battery Charge / Discharge", "Battery draw and store power", ["battery_draw_final_kw", "battery_store_final_kw"]),
        ]
    if path.name == "base_case_profile_compliance_monthly.csv":
        return [
            _chart_card(
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
                _chart_card(
                    battery_cases,
                    "Grid Import vs Capacity",
                    "Named-case grid import by battery capacity",
                    ["grid_import_kw_min"],
                    x_column="battery_capacity_kwh",
                )
            )
            charts.append(
                _chart_card(
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
                _chart_card(
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
                _chart_card(
                    by_capacity,
                    "Cross Grid Import vs Capacity",
                    "Average cross-case grid import by battery capacity",
                    ["grid_import_kw_min"],
                    x_column="battery_capacity_kwh",
                )
            )
            charts.append(
                _chart_card(
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
                _chart_card(
                    by_profile,
                    "Cross Annual Gap vs Profile Multiplier",
                    "Average cross-case annual gap by profile multiplier",
                    ["annual_energy_gap_kwh"],
                    x_column="profile_multiplier",
                )
            )
        return [chart for chart in charts if chart.svg]
    return [ChartCard("Preview Chart", "Auto-selected numeric columns", build_chart_svg_from_df(frame) or "")]


def build_chart_svg_from_df(
    df: pl.DataFrame,
    preferred_columns: list[str] | None = None,
    x_column: str = "timestamp",
    width: int = 1100,
    height: int = 360,
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

    if df.height > 360:
        step = max(1, math.ceil(df.height / 360))
        df = df.gather_every(step)

    left_padding = 66
    right_padding = 18
    top_padding = 38
    bottom_padding = 44
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
            tooltip_x = max(min(x, width - 125), 115)
            tooltip_y = max(y - 18, 58)
            hover_svg.append(
                f'<g class="chart-point-group">'
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="6" fill="transparent" class="chart-point-hover" />'
                f'<g class="chart-tooltip-group">'
                f'<rect x="{tooltip_x - 105:.2f}" y="{tooltip_y - 42:.2f}" width="210" height="36" fill="#0f172a" fill-opacity="0.92" rx="6" />'
                f'<text x="{tooltip_x:.2f}" y="{tooltip_y - 26:.2f}" text-anchor="middle" fill="#94a3b8" font-size="10">{html.escape(str(original_x_values[index]))}</text>'
                f'<text x="{tooltip_x:.2f}" y="{tooltip_y - 10:.2f}" text-anchor="middle" fill="#f8fafc" font-size="11" font-weight="600">{html.escape(column)}: {_format_number(value)}</text>'
                f"</g>"
                f"</g>"
            )
        legend_x = left_padding + (color_index * 155)
        series_svg.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{" ".join(points)}" />')
        series_svg.append(
            f'<rect x="{legend_x}" y="10" width="10" height="10" rx="2" fill="{color}" />'
            f'<text x="{legend_x + 16}" y="19" fill="#334155" font-size="11" font-weight="600">{html.escape(column)}</text>'
        )

    ticks: list[str] = []
    for tick_value in _build_y_ticks(max_value):
        y = height - bottom_padding - ((tick_value / max_value) * chart_height)
        ticks.append(f'<line x1="{left_padding}" y1="{y:.2f}" x2="{width - right_padding}" y2="{y:.2f}" stroke="#e2e8f0" stroke-width="1" />')
        ticks.append(f'<text x="{left_padding - 8}" y="{y + 4:.2f}" text-anchor="end" fill="#64748b" font-size="10">{html.escape(_format_tick_value(tick_value))}</text>')
    for tick_value, tick_label in zip(x_tick_values, x_tick_labels, strict=True):
        x = left_padding + ((tick_value - x_min) / x_span) * chart_width
        ticks.append(f'<line x1="{x:.2f}" y1="{height - bottom_padding}" x2="{x:.2f}" y2="{height - bottom_padding + 5}" stroke="#cbd5e1" stroke-width="1" />')
        ticks.append(f'<text x="{x:.2f}" y="{height - bottom_padding + 16}" text-anchor="middle" fill="#64748b" font-size="10">{html.escape(tick_label)}</text>')

    return (
        f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="xMidYMid meet" class="chart-svg">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" rx="16" />'
        f'<line x1="{left_padding}" y1="{height - bottom_padding}" x2="{width - right_padding}" y2="{height - bottom_padding}" stroke="#cbd5e1" stroke-width="1.5" />'
        f'<line x1="{left_padding}" y1="{top_padding}" x2="{left_padding}" y2="{height - bottom_padding}" stroke="#cbd5e1" stroke-width="1.5" />'
        f'{"".join(ticks)}'
        f'<text x="{left_padding + chart_width / 2:.2f}" y="{height - 8}" text-anchor="middle" fill="#475569" font-weight="500" font-size="11">{html.escape(x_axis_label)}</text>'
        f'<text x="18" y="{top_padding + chart_height / 2:.2f}" text-anchor="middle" fill="#475569" font-weight="500" font-size="11" transform="rotate(-90 18 {top_padding + chart_height / 2:.2f})">{html.escape(y_axis_label)}</text>'
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
) -> ChartCard:
    return ChartCard(title=title, subtitle=subtitle, svg=build_chart_svg_from_df(df, columns, x_column=x_column) or "")


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
    return (_repo_root() / "config" / "project.yaml").resolve()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


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
