"""Sensitivity scenario generation and execution."""

from __future__ import annotations

from itertools import product
from typing import Callable

from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.core.pipeline import simulate_system

ScenarioProgressCallback = Callable[[int, int, str], None]


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
    rows: list[dict[str, float | str | int | None]] = []
    total = len(scenarios)
    for index, scenario in enumerate(scenarios, start=1):
        if progress_callback:
            progress_callback(index - 1, total, str(scenario["case_id"]))

        run_config = config.build_simulation_variant(
            wind_multiplier=float(scenario["wind_multiplier"]),
            solar_multiplier=float(scenario["solar_multiplier"]),
            profile_multiplier=float(scenario["profile_multiplier"]),
            battery_capacity_kwh=float(scenario["capacity_kwh"]),
            battery_duration_hours=float(scenario["duration_hours"]),
        )
        result = simulate_system(run_config)
        rows.append(
            {
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
        )

        if progress_callback:
            progress_callback(index, total, str(scenario["case_id"]))
    return rows


def _format_multiplier(value: float) -> str:
    return f"{value:.2f}".replace(".", "_")


def _format_hours(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".").replace(".", "_")
