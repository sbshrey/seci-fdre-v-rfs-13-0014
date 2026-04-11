"""Sensitivity scenario generation and execution."""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import product
from typing import Callable, cast

from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.core.pipeline import simulate_system

ScenarioProgressCallback = Callable[[int, int, str], None]


def _resolve_scenario_workers() -> int:
    """Worker count for parallel sensitivity runs (process pool).

    ``SECI_FDRE_V_SCENARIO_WORKERS``:
    - unset or ``auto``: ``min(8, max(1, os.cpu_count() or 4))``
    - ``1``: sequential execution (no process pool)
    - positive integer: cap at 32
    """
    raw = os.environ.get("SECI_FDRE_V_SCENARIO_WORKERS", "").strip().lower()
    cpu = os.cpu_count() or 4
    auto = max(1, min(8, cpu))
    if raw in ("", "auto"):
        return auto
    try:
        n = int(raw)
    except ValueError:
        return auto
    if n < 1:
        return auto
    return min(n, 32)


def _simulate_scenario_worker(
    config: ProjectConfig,
    scenario: dict[str, float | str | int | None],
) -> dict[str, float | str | int | None]:
    """Run one sensitivity case; module-level for pickling under ``ProcessPoolExecutor``."""
    run_config = config.build_simulation_variant(
        wind_multiplier=float(scenario["wind_multiplier"]),
        solar_multiplier=float(scenario["solar_multiplier"]),
        profile_multiplier=float(scenario["profile_multiplier"]),
        battery_capacity_kwh=float(scenario["capacity_kwh"]),
        battery_duration_hours=float(scenario["duration_hours"]),
    )
    result = simulate_system(run_config)
    return {
        "case_id": str(scenario["case_id"]),
        "case_group": str(scenario["case_group"]),
        "wind_multiplier": float(scenario["wind_multiplier"]),
        "solar_multiplier": float(scenario["solar_multiplier"]),
        "profile_multiplier": float(scenario["profile_multiplier"]),
        "battery_capacity_kwh": float(scenario["capacity_kwh"]),
        "battery_duration_hours": float(scenario["duration_hours"]),
        "battery_power_kw": run_config.battery.nominal_power_kw,
        **dict(result.summary_metrics),
    }


def build_case_rows(
    config: ProjectConfig,
    *,
    progress_callback: ScenarioProgressCallback | None = None,
) -> list[dict[str, float | str | int | None]]:
    scenarios: list[dict[str, float | str | int | None]] = [
        _scenario(
            case_id="base",
            case_group="base",
            wind_multiplier=1.0,
            solar_multiplier=1.0,
            profile_multiplier=1.0,
            capacity_kwh=config.simulation.battery.capacity_kwh,
            duration_hours=config.simulation.battery.duration_hours,
        )
    ]

    for factor_name, values in (
        ("wind", config.sensitivity.wind_multipliers),
        ("solar", config.sensitivity.solar_multipliers),
        ("profile", config.sensitivity.profile_multipliers),
    ):
        for value in values:
            if value == 1.0:
                continue
            scenarios.append(
                _scenario(
                    case_id=f"{factor_name}_{_format_multiplier(value)}",
                    case_group=factor_name,
                    wind_multiplier=value if factor_name == "wind" else 1.0,
                    solar_multiplier=value if factor_name == "solar" else 1.0,
                    profile_multiplier=value if factor_name == "profile" else 1.0,
                    capacity_kwh=config.simulation.battery.capacity_kwh,
                    duration_hours=config.simulation.battery.duration_hours,
                )
            )

    for capacity_kwh in config.sensitivity.battery_capacity_kwh_values:
        if capacity_kwh != config.simulation.battery.capacity_kwh:
            scenarios.append(
                _scenario(
                    case_id=f"battery_capacity_{int(round(capacity_kwh))}",
                    case_group="battery_capacity",
                    wind_multiplier=1.0,
                    solar_multiplier=1.0,
                    profile_multiplier=1.0,
                    capacity_kwh=capacity_kwh,
                    duration_hours=config.simulation.battery.duration_hours,
                )
            )

    for duration_hours in config.sensitivity.battery_duration_hour_values:
        if duration_hours != config.simulation.battery.duration_hours:
            scenarios.append(
                _scenario(
                    case_id=f"battery_hours_{_format_hours(duration_hours)}",
                    case_group="battery_hours",
                    wind_multiplier=1.0,
                    solar_multiplier=1.0,
                    profile_multiplier=1.0,
                    capacity_kwh=config.simulation.battery.capacity_kwh,
                    duration_hours=duration_hours,
                )
            )

    return _run_scenarios(config, scenarios, progress_callback=progress_callback)


def build_cross_table_rows(
    config: ProjectConfig,
    *,
    progress_callback: ScenarioProgressCallback | None = None,
) -> list[dict[str, float | str | int | None]]:
    scenarios: list[dict[str, float | str | int | None]] = []
    for wind_multiplier, solar_multiplier, profile_multiplier, capacity_kwh, duration_hours in product(
        config.sensitivity.wind_multipliers,
        config.sensitivity.solar_multipliers,
        config.sensitivity.profile_multipliers,
        config.sensitivity.battery_capacity_kwh_values,
        config.sensitivity.battery_duration_hour_values,
    ):
        scenarios.append(
            _scenario(
                case_id=f"w{_format_multiplier(wind_multiplier)}_s{_format_multiplier(solar_multiplier)}_p{_format_multiplier(profile_multiplier)}_c{int(round(capacity_kwh))}_h{_format_hours(duration_hours)}",
                case_group="cross",
                wind_multiplier=wind_multiplier,
                solar_multiplier=solar_multiplier,
                profile_multiplier=profile_multiplier,
                capacity_kwh=capacity_kwh,
                duration_hours=duration_hours,
            )
        )

    return _run_scenarios(config, scenarios, progress_callback=progress_callback)


def _scenario(
    *,
    case_id: str,
    case_group: str,
    wind_multiplier: float,
    solar_multiplier: float,
    profile_multiplier: float,
    capacity_kwh: float,
    duration_hours: float,
) -> dict[str, float | str | int | None]:
    return {
        "case_id": case_id,
        "case_group": case_group,
        "wind_multiplier": wind_multiplier,
        "solar_multiplier": solar_multiplier,
        "profile_multiplier": profile_multiplier,
        "capacity_kwh": capacity_kwh,
        "duration_hours": duration_hours,
    }


def _run_scenarios(
    config: ProjectConfig,
    scenarios: list[dict[str, float | str | int | None]],
    *,
    progress_callback: ScenarioProgressCallback | None = None,
) -> list[dict[str, float | str | int | None]]:
    total = len(scenarios)
    workers = _resolve_scenario_workers()
    if workers <= 1 or total <= 1:
        return _run_scenarios_sequential(config, scenarios, progress_callback=progress_callback)

    if progress_callback:
        progress_callback(0, total, str(scenarios[0]["case_id"]))

    rows: list[dict[str, float | str | int | None] | None] = [None] * total
    executor = ProcessPoolExecutor(max_workers=workers)
    abnormal = True
    try:
        future_to_index = {
            executor.submit(_simulate_scenario_worker, config, scenario): index
            for index, scenario in enumerate(scenarios)
        }
        completed = 0
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            rows[index] = future.result()
            completed += 1
            if progress_callback:
                progress_callback(completed, total, str(scenarios[index]["case_id"]))
        abnormal = False
    finally:
        executor.shutdown(wait=not abnormal, cancel_futures=abnormal)

    assert all(row is not None for row in rows)
    return cast(list[dict[str, float | str | int | None]], rows)


def _run_scenarios_sequential(
    config: ProjectConfig,
    scenarios: list[dict[str, float | str | int | None]],
    *,
    progress_callback: ScenarioProgressCallback | None = None,
) -> list[dict[str, float | str | int | None]]:
    rows: list[dict[str, float | str | int | None]] = []
    total = len(scenarios)
    for index, scenario in enumerate(scenarios, start=1):
        if progress_callback:
            progress_callback(index - 1, total, str(scenario["case_id"]))

        rows.append(_simulate_scenario_worker(config, scenario))

        if progress_callback:
            progress_callback(index, total, str(scenario["case_id"]))
    return rows


def _format_multiplier(value: float) -> str:
    return f"{value:.2f}".replace(".", "_")


def _format_hours(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".").replace(".", "_")
