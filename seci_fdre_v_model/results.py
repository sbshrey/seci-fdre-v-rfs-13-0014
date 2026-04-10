"""Typed return models for simulation and sizing runs."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class SimulationResult:
    """Minute-level outputs and aggregate metrics for a single run."""

    minute_flows: pl.DataFrame
    summary_metrics: dict[str, float | int | str | None]
    profile_compliance_blocks: pl.DataFrame | None = None
    profile_compliance_monthly: pl.DataFrame | None = None
