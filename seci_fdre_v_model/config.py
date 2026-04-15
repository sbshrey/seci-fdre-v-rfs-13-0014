"""Configuration models and YAML loading helpers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

DEFAULT_DEGRADATION_PER_CYCLE = 0.0002739726027
DEFAULT_CHARGE_LOSS_TABLE = {
    0.0: 0.0,
    0.1: 0.04,
    0.2: 0.04,
    0.3: 0.045,
    0.4: 0.0575,
    0.5: 0.07,
    0.6: 0.078,
    0.7: 0.086,
    0.8: 0.094,
    0.9: 0.102,
    1.0: 0.11,
    1.1: 0.125,
    1.2: 0.14,
    1.3: 0.14,
    1.4: 0.14,
    1.5: 0.14,
}
DEFAULT_DISCHARGE_LOSS_TABLE = {
    0.0: 0.0,
    0.1: 0.023,
    0.2: 0.023,
    0.3: 0.032,
    0.4: 0.037,
    0.5: 0.042,
    0.6: 0.046,
    0.7: 0.05,
    0.8: 0.054,
    0.9: 0.058,
    1.0: 0.062,
    1.1: 0.0645,
    1.2: 0.067,
    1.3: 0.067,
    1.4: 0.067,
    1.5: 0.067,
}


@dataclass(frozen=True)
class ProjectSettings:
    plant_name: str
    output_dir: str = "output"
    simulation_start: datetime = datetime(2025, 1, 1, 0, 0)
    simulation_end: datetime = datetime(2025, 12, 31, 23, 59)


@dataclass(frozen=True)
class InputFilesConfig:
    solar_path: Path
    wind_path: Path
    output_profile_path: Path
    output_profile_18_22_path: Path
    aux_power_path: Path | None


@dataclass(frozen=True)
class DataConfig:
    solar_path: str
    wind_path: str
    solar_enabled: bool = True
    wind_enabled: bool = True
    wind_multiplier: float = 1.0
    solar_multiplier: float = 1.0


@dataclass(frozen=True)
class PreprocessingConfig:
    frequency: str = "1m"
    gap_fill: str = "linear_interpolate"
    max_interpolation_gap_minutes: int = 15
    align_to_full_year: bool = True
    simulation_dtype: str = "float32"


@dataclass(frozen=True)
class GridConfig:
    export_limit_kw: float
    import_limit_kw: float | None = None


@dataclass(frozen=True)
class LoadConfig:
    output_profile_kw: float | None = None
    aux_consumption_kw: float = 0.0
    aux_mode: str = "static_csv"
    aux_charge_fraction: float | None = None
    aux_discharge_fraction: float | None = None
    aux_idle_fraction: float | None = None
    profile_mode: str = "template"
    profile_template_id: str | None = None
    contracted_capacity_mw: float | None = None
    output_profile_path: str | None = None
    aux_power_path: str | None = None
    profile_multiplier: float = 1.0

    @property
    def uses_template_profile(self) -> bool:
        return self.profile_mode == "template"

    @property
    def uses_static_aux(self) -> bool:
        return self.aux_mode == "static_csv"

    @property
    def uses_battery_state_aux(self) -> bool:
        return self.aux_mode == "battery_state"


@dataclass(frozen=True)
class BatteryConfig:
    nominal_power_kw: float
    duration_hours: float
    initial_soc_fraction: float = 1.0
    degradation_per_cycle: float = DEFAULT_DEGRADATION_PER_CYCLE
    charge_loss_table: dict[float, float] = field(default_factory=lambda: dict(DEFAULT_CHARGE_LOSS_TABLE))
    discharge_loss_table: dict[float, float] = field(default_factory=lambda: dict(DEFAULT_DISCHARGE_LOSS_TABLE))
    min_soc_fraction: float = 0.0
    max_soc_fraction: float = 1.0
    max_charge_kw: float = 0.0
    max_discharge_kw: float = 0.0
    charge_efficiency: float = 1.0
    discharge_efficiency: float = 1.0

    @property
    def capacity_kwh(self) -> float:
        return self.nominal_power_kw * self.duration_hours

    def with_capacity_and_duration(self, capacity_kwh: float, duration_hours: float) -> "BatteryConfig":
        nominal_power_kw = capacity_kwh / duration_hours if duration_hours > 0 else capacity_kwh
        return replace(
            self,
            nominal_power_kw=nominal_power_kw,
            duration_hours=duration_hours,
            max_charge_kw=nominal_power_kw,
            max_discharge_kw=nominal_power_kw,
        )


@dataclass(frozen=True)
class SensitivityConfig:
    wind_multipliers: list[float] = field(default_factory=lambda: [0.8, 0.9, 1.0, 1.1, 1.2])
    solar_multipliers: list[float] = field(default_factory=lambda: [0.8, 0.9, 1.0, 1.1, 1.2])
    profile_multipliers: list[float] = field(default_factory=lambda: [0.8, 0.9, 1.0, 1.1, 1.2])
    battery_capacity_kwh_values: list[float] = field(default_factory=lambda: [500_000.0, 1_000_000.0])
    battery_duration_hour_values: list[float] = field(default_factory=lambda: [2.0, 4.0])


@dataclass(frozen=True)
class SimulationConfig:
    plant_name: str
    data: DataConfig
    preprocessing: PreprocessingConfig
    grid: GridConfig
    load: LoadConfig
    battery: BatteryConfig
    output_dir: str = "output"

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SimulationConfig":
        config = cls(
            plant_name=str(payload["plant_name"]),
            data=_normalize_data_config(payload.get("data") or {}),
            preprocessing=PreprocessingConfig(**dict(payload.get("preprocessing", {}))),
            grid=GridConfig(**dict(payload["grid"])),
            load=_normalize_load_config(payload.get("load") or {}),
            battery=_normalize_battery_config(payload["battery"]),
            output_dir=str(payload.get("output_dir", "output")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        from seci_fdre_v_model.profile_templates import SUPPORTED_TENDER_PROFILES

        if not (self.data.solar_enabled or self.data.wind_enabled):
            raise ValueError("At least one of data.solar_enabled or data.wind_enabled must be True.")
        if self.grid.export_limit_kw is None or self.grid.export_limit_kw <= 0:
            raise ValueError("grid.export_limit_kw must be positive.")
        if self.grid.import_limit_kw is not None and self.grid.import_limit_kw < 0:
            raise ValueError("grid.import_limit_kw must be non-negative when provided.")
        if self.preprocessing.frequency != "1m":
            raise ValueError("Only 1-minute simulation frequency is supported.")
        if self.preprocessing.max_interpolation_gap_minutes < 0:
            raise ValueError("max_interpolation_gap_minutes must be non-negative.")
        if self.load.profile_mode not in {"flat", "template"}:
            raise ValueError("load.profile_mode must be either 'flat' or 'template'.")
        if self.load.aux_mode not in {"static_csv", "battery_state"}:
            raise ValueError("load.aux_mode must be either 'static_csv' or 'battery_state'.")
        if self.load.profile_mode == "flat":
            if self.load.output_profile_path is None:
                raise ValueError("load.output_profile_path is required in flat profile mode.")
        else:
            if not self.load.profile_template_id:
                raise ValueError("load.profile_template_id is required in template profile mode.")
            if self.load.profile_template_id not in SUPPORTED_TENDER_PROFILES:
                supported = ", ".join(sorted(SUPPORTED_TENDER_PROFILES))
                raise ValueError(
                    f"Unsupported load.profile_template_id '{self.load.profile_template_id}'. Expected one of: {supported}."
                )
            if self.load.contracted_capacity_mw is None or self.load.contracted_capacity_mw <= 0:
                raise ValueError("load.contracted_capacity_mw must be positive in template profile mode.")
        if self.load.output_profile_path is None:
            raise ValueError("load.output_profile_path must be set.")
        if self.load.uses_static_aux:
            if self.load.aux_power_path is None:
                raise ValueError("load.aux_power_path must be set in static_csv aux mode.")
        else:
            required_fractions = {
                "load.aux_charge_fraction": self.load.aux_charge_fraction,
                "load.aux_discharge_fraction": self.load.aux_discharge_fraction,
                "load.aux_idle_fraction": self.load.aux_idle_fraction,
            }
            missing = [name for name, value in required_fractions.items() if value is None]
            if missing:
                raise ValueError(f"Missing required battery_state aux fields: {', '.join(missing)}.")
            negative = [name for name, value in required_fractions.items() if value is not None and value < 0]
            if negative:
                raise ValueError(f"Battery-state aux fractions must be non-negative: {', '.join(negative)}.")
        if self.data.solar_enabled and not Path(self.data.solar_path).exists():
            raise FileNotFoundError(f"Solar file not found: {self.data.solar_path}")
        if self.data.wind_enabled and not Path(self.data.wind_path).exists():
            raise FileNotFoundError(f"Wind file not found: {self.data.wind_path}")


@dataclass(frozen=True)
class ProjectConfig:
    project: ProjectSettings
    inputs: InputFilesConfig
    simulation: SimulationConfig
    sensitivity: SensitivityConfig
    config_path: Path

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ProjectConfig":
        config_path = Path(path).expanduser().resolve()
        with config_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
        if not isinstance(payload, dict):
            raise ValueError("Configuration root must be a mapping.")

        project_payload = dict(payload.get("project", {}))
        project = ProjectSettings(
            plant_name=str(project_payload.get("plant_name", "seci_fdre_v_plant")),
            output_dir=str(project_payload.get("output_dir", "output")),
            simulation_start=_parse_datetime(project_payload.get("simulation_start", "2025-01-01 00:00")),
            simulation_end=_parse_datetime(project_payload.get("simulation_end", "2025-12-31 23:59")),
        )

        inputs_payload = dict(payload.get("inputs", {}))
        base_dir = config_path.parent
        inputs = InputFilesConfig(
            solar_path=_resolve_path(base_dir, inputs_payload.get("solar_path", "../data/Solar_2025-01-01_data_.csv")),
            wind_path=_resolve_path(base_dir, inputs_payload.get("wind_path", "../data/Wind_2025_01-01_data_.csv")),
            output_profile_path=_resolve_path(base_dir, inputs_payload.get("output_profile_path", "../data/seci_fdre_v_amendment_03_output_profile.csv")),
            output_profile_18_22_path=_resolve_path(base_dir, inputs_payload.get("output_profile_18_22_path", "../data/seci_fdre_v_amendment_03_output_profile_18_22.csv")),
            aux_power_path=(
                _resolve_path(base_dir, inputs_payload["aux_power_path"])
                if inputs_payload.get("aux_power_path") not in (None, "")
                else None
            ),
        )

        simulation_payload = dict(payload.get("simulation", {}))
        data_payload = dict(simulation_payload.get("data", {}))
        data_payload.setdefault("solar_path", str(inputs.solar_path))
        data_payload.setdefault("wind_path", str(inputs.wind_path))
        load_payload = dict(simulation_payload.get("load", {}))
        load_payload.setdefault("output_profile_path", str(inputs.output_profile_path))
        load_payload.setdefault("aux_power_path", str(inputs.aux_power_path) if inputs.aux_power_path is not None else None)

        sim_config = SimulationConfig.from_dict(
            {
                "plant_name": project.plant_name,
                "output_dir": project.output_dir,
                "data": data_payload,
                "preprocessing": simulation_payload.get("preprocessing", {}),
                "grid": simulation_payload["grid"],
                "load": load_payload,
                "battery": simulation_payload["battery"],
            }
        )

        sensitivity_payload = dict(payload.get("sensitivity", {}))
        sensitivity = SensitivityConfig(
            wind_multipliers=_float_list(sensitivity_payload.get("wind_multipliers"), [0.8, 0.9, 1.0, 1.1, 1.2]),
            solar_multipliers=_float_list(sensitivity_payload.get("solar_multipliers"), [0.8, 0.9, 1.0, 1.1, 1.2]),
            profile_multipliers=_float_list(sensitivity_payload.get("profile_multipliers"), [0.8, 0.9, 1.0, 1.1, 1.2]),
            battery_capacity_kwh_values=_float_list(sensitivity_payload.get("battery_capacity_kwh_values"), [sim_config.battery.capacity_kwh]),
            battery_duration_hour_values=_float_list(sensitivity_payload.get("battery_duration_hour_values"), [sim_config.battery.duration_hours]),
        )
        return cls(project=project, inputs=inputs, simulation=sim_config, sensitivity=sensitivity, config_path=config_path)

    def build_simulation_variant(
        self,
        *,
        wind_multiplier: float,
        solar_multiplier: float,
        profile_multiplier: float,
        battery_capacity_kwh: float,
        battery_duration_hours: float,
    ) -> SimulationConfig:
        variant = replace(
            self.simulation,
            data=replace(self.simulation.data, wind_multiplier=wind_multiplier, solar_multiplier=solar_multiplier),
            load=replace(self.simulation.load, profile_multiplier=profile_multiplier),
            battery=self.simulation.battery.with_capacity_and_duration(battery_capacity_kwh, battery_duration_hours),
        )
        variant.validate()
        return variant


def _normalize_data_config(payload: dict[str, Any]) -> DataConfig:
    if "solar_path" not in payload or "wind_path" not in payload:
        raise ValueError("simulation.data.solar_path and simulation.data.wind_path are required.")
    return DataConfig(
        solar_path=str(payload["solar_path"]),
        wind_path=str(payload["wind_path"]),
        solar_enabled=bool(payload.get("solar_enabled", True)),
        wind_enabled=bool(payload.get("wind_enabled", True)),
        wind_multiplier=float(payload.get("wind_multiplier", 1.0)),
        solar_multiplier=float(payload.get("solar_multiplier", 1.0)),
    )


def _normalize_load_config(payload: dict[str, Any]) -> LoadConfig:
    return LoadConfig(
        output_profile_kw=float(payload["output_profile_kw"]) if payload.get("output_profile_kw") not in (None, "") else None,
        aux_consumption_kw=float(payload.get("aux_consumption_kw", 0.0)),
        aux_mode=str(payload.get("aux_mode", "static_csv")),
        aux_charge_fraction=float(payload["aux_charge_fraction"]) if payload.get("aux_charge_fraction") not in (None, "") else None,
        aux_discharge_fraction=float(payload["aux_discharge_fraction"]) if payload.get("aux_discharge_fraction") not in (None, "") else None,
        aux_idle_fraction=float(payload["aux_idle_fraction"]) if payload.get("aux_idle_fraction") not in (None, "") else None,
        profile_mode=str(payload.get("profile_mode", "template")),
        profile_template_id=str(payload["profile_template_id"]) if payload.get("profile_template_id") not in (None, "") else None,
        contracted_capacity_mw=float(payload["contracted_capacity_mw"]) if payload.get("contracted_capacity_mw") not in (None, "") else None,
        output_profile_path=str(payload["output_profile_path"]) if payload.get("output_profile_path") not in (None, "") else None,
        aux_power_path=str(payload["aux_power_path"]) if payload.get("aux_power_path") not in (None, "") else None,
        profile_multiplier=float(payload.get("profile_multiplier", 1.0)),
    )


def _normalize_battery_config(payload: dict[str, Any]) -> BatteryConfig:
    normalized = dict(payload)
    nominal_power_kw = float(normalized["nominal_power_kw"])
    duration_hours = float(normalized["duration_hours"])
    charge_loss_table = _normalize_loss_table(normalized.get("charge_loss_table", DEFAULT_CHARGE_LOSS_TABLE))
    discharge_loss_table = _normalize_loss_table(normalized.get("discharge_loss_table", DEFAULT_DISCHARGE_LOSS_TABLE))
    return BatteryConfig(
        nominal_power_kw=nominal_power_kw,
        duration_hours=duration_hours,
        initial_soc_fraction=float(normalized.get("initial_soc_fraction", 1.0)),
        degradation_per_cycle=float(normalized.get("degradation_per_cycle", DEFAULT_DEGRADATION_PER_CYCLE)),
        charge_loss_table=charge_loss_table,
        discharge_loss_table=discharge_loss_table,
        min_soc_fraction=float(normalized.get("min_soc_fraction", 0.0)),
        max_soc_fraction=float(normalized.get("max_soc_fraction", 1.0)),
        max_charge_kw=float(normalized.get("max_charge_kw", nominal_power_kw)),
        max_discharge_kw=float(normalized.get("max_discharge_kw", nominal_power_kw)),
        charge_efficiency=float(normalized.get("charge_efficiency", 1.0 - charge_loss_table.get(1.0, 0.0))),
        discharge_efficiency=float(normalized.get("discharge_efficiency", 1.0 - discharge_loss_table.get(1.0, 0.0))),
    )


def _normalize_loss_table(raw: dict[Any, Any]) -> dict[float, float]:
    return {float(key): float(value) for key, value in raw.items()}


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d %H:%M")


def _float_list(values: Any, default: list[float]) -> list[float]:
    if values is None:
        return list(default)
    return [float(value) for value in values]
