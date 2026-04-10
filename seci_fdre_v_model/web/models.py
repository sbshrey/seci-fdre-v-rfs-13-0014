"""App-layer view and workspace models for the local control room."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkspaceState:
    root: Path
    config_dir: Path
    config_path: Path
    inputs_dir: Path
    runs_dir: Path
    metadata_path: Path
    source_config_path: Path


@dataclass(frozen=True)
class ManagedInput:
    key: str
    label: str
    canonical_name: str
    absolute_path: Path
    expected_headers: tuple[str, ...]
    description: str
    exists: bool
    original_name: str | None = None
    source: str | None = None
    modified_at: str | None = None
    size_kb: float | None = None
    validation_ok: bool = False
    validation_message: str = ""


@dataclass(frozen=True)
class RunArtifactIndex:
    relative_path: str
    absolute_path: Path
    size_kb: float
    modified_at: str
    is_tabular: bool


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    run_dir: Path
    package_dir: Path
    config_path: Path
    status: str
    plant_name: str
    started_at: str
    finished_at: str | None
    artifacts: list[RunArtifactIndex] = field(default_factory=list)
    summary_metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class MetricCard:
    title: str
    value: str
    subtitle: str


@dataclass(frozen=True)
class EnergyTableRow:
    category: str
    element: str
    value_kw_min: float


@dataclass(frozen=True)
class ChartCard:
    title: str
    subtitle: str
    svg: str


@dataclass(frozen=True)
class TablePreview:
    columns: list[str]
    rows: list[dict[str, Any]]
    page: int
    page_size: int
    total_rows: int
    total_pages: int


@dataclass(frozen=True)
class BackgroundJob:
    run_id: str | None
    status: str
    stage: str
    pct: float
    detail: str
    completed_cases: int | None
    total_cases: int | None
    current_case_id: str | None
    started_at: str | None
    updated_at: str | None
    finished_at: str | None
    error: str | None = None
    cancel_requested: bool = False

    @property
    def is_active(self) -> bool:
        return self.status in {"queued", "running", "cancelling"}
