"""Section-based accounting stage and CSV exports."""

from __future__ import annotations

import csv as csv_module
import gc
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable
import bisect

import numpy as np
import polars as pl
from seci_fdre_v_model.profile_templates import build_load_profile_frame

if TYPE_CHECKING:
    from seci_fdre_v_model.config import BatteryConfig
    from seci_fdre_v_model.core.pipeline import SimulationContext

IDENTITY_TOLERANCE_FLOAT64 = 1e-3
IDENTITY_TOLERANCE_FLOAT32 = 1e-2


@dataclass(frozen=True)
class OutputSection:
    """One section CSV export."""

    file_name: str
    title: str
    columns: tuple[str, ...]


OUTPUT_SECTIONS: tuple[OutputSection, ...] = (
    OutputSection("01_wind_solar_generation.csv", "Wind & Solar Generation", ("timestamp", "wind_kw", "solar_kw", "total_generation_kw")),
    OutputSection(
        "02_cumulative_generation.csv",
        "Cumulative Generation",
        ("timestamp", "cum_wind_kw_min", "cum_solar_kw_min", "cum_total_kw_min"),
    ),
    OutputSection(
        "03_output_profile.csv",
        "Output Profile",
        ("timestamp", "output_profile_kw", "aux_consumption_kw", "total_consumption_kw"),
    ),
    OutputSection(
        "04_battery_capacity_cycles.csv",
        "Battery Capacity Based on Cycles",
        ("timestamp", "current_cycle", "cumulative_degradation", "capacity_now_kw_min"),
    ),
    OutputSection("05_excess_deficit_power.csv", "Excess or Deficit Power", ("timestamp", "excess_power_kw", "deficit_power_kw")),
    OutputSection(
        "06_battery_opening_closing.csv",
        "Battery Opening or Closing",
        ("timestamp", "battery_opening_kw_min", "battery_closing_kw_min"),
    ),
    OutputSection(
        "07_power_from_battery.csv",
        "Power from Battery",
        (
            "timestamp",
            "battery_draw_required_kw",
            "battery_draw_c_rate",
            "battery_draw_loss_rate",
            "battery_draw_loss_kw",
            "battery_draw_final_kw",
            "battery_draw_cumulative_kw_min",
        ),
    ),
    OutputSection("08_consume_from_grid.csv", "Consume from Grid", ("timestamp", "grid_buy_kw")),
    OutputSection(
        "09_power_to_battery.csv",
        "Power to Battery",
        (
            "timestamp",
            "battery_store_available_kw",
            "battery_store_c_rate",
            "battery_store_loss_rate",
            "battery_store_loss_kw",
            "battery_store_final_kw",
            "battery_store_cumulative_kw_min",
        ),
    ),
    OutputSection("10_sell_to_grid.csv", "Sell to Grid", ("timestamp", "grid_sell_kw")),
    OutputSection("11_soc_calculations.csv", "SOC Calculations", ("timestamp", "soc_kw_min", "soc_fraction", "soc_pct")),
    OutputSection(
        "12_battery_charge_cycles.csv",
        "Number of Battery Charge Cycles",
        ("timestamp", "discharge_cycle_count", "charge_cycle_count", "cum_charge_count"),
    ),
    OutputSection(
        "13_identity_equation_1.csv",
        "Identity Equation 1",
        ("timestamp", "energy_sources_kw", "energy_uses_kw", "energy_losses_kw", "identity_1_error_kw", "identity_1_ok"),
    ),
    OutputSection(
        "14_identity_equation_2.csv",
        "Identity Equation 2",
        (
            "timestamp",
            "bess_start_kw_min",
            "bess_discharge_kw",
            "bess_discharge_loss_kw",
            "bess_charge_kw",
            "bess_charge_loss_kw",
            "bess_finish_kw_min",
            "battery_closing_kw_min",
            "identity_2_error_kw_min",
            "identity_2_ok",
        ),
    ),
)


def section_accounting_stage(df: pl.DataFrame, context: SimulationContext) -> pl.DataFrame:
    """Append the canonical section accounting columns."""
    total_generation = df["total_generation_kw"].to_numpy()
    wind = df["wind_kw"].to_numpy()
    solar = df["solar_kw"].to_numpy()
    if "output_profile_kw" in df.columns:
        output_profile = df["output_profile_kw"].to_numpy()
    if "aux_consumption_kw" in df.columns:
        aux_consumption = df["aux_consumption_kw"].to_numpy()
    if "total_consumption_kw" in df.columns:
        total_consumption = df["total_consumption_kw"].to_numpy()
    elif "site_load_kw" in df.columns:
        total_consumption = df["site_load_kw"].to_numpy()
    else:
        load_frame = build_load_profile_frame(df["timestamp"], context.config.load)
        output_profile = load_frame["output_profile_kw"].to_numpy()
        aux_consumption = load_frame["aux_consumption_kw"].to_numpy()
        total_consumption = load_frame["total_consumption_kw"].to_numpy()
    if "output_profile_kw" in df.columns and "aux_consumption_kw" not in df.columns:
        aux_consumption = np.full(df.height, context.config.load.aux_consumption_kw, dtype=np.float64)
    if "output_profile_kw" in df.columns and "total_consumption_kw" not in df.columns and "site_load_kw" not in df.columns:
        total_consumption = output_profile + aux_consumption
    progress_cb = getattr(context, "progress_callback", None) or (
        getattr(context, "_progress", None)
    )
    preprocessing = context.config.preprocessing
    metrics = _simulate_section_accounting(
        total_generation,
        total_consumption,
        wind,
        solar,
        context.config.battery,
        dtype=preprocessing.simulation_dtype,
        identity_tolerance=_identity_tolerance(preprocessing.simulation_dtype),
        progress_callback=progress_cb,
    )
    del total_generation, wind, solar
    gc.collect()

    result = df.with_columns(
        pl.Series("output_profile_kw", output_profile),
        pl.Series("aux_consumption_kw", aux_consumption),
        pl.Series("total_consumption_kw", total_consumption),
        pl.Series("cum_wind_kw_min", metrics["cum_wind"]),
        pl.Series("cum_solar_kw_min", metrics["cum_solar"]),
        pl.Series("cum_total_kw_min", metrics["cum_total"]),
        pl.Series("current_cycle", metrics["current_cycle"]),
        pl.Series("cumulative_degradation", metrics["cumulative_degradation"]),
        pl.Series("capacity_now_kw_min", metrics["capacity_now_kw_min"]),
        pl.Series("excess_power_kw", metrics["excess_power_kw"]),
        pl.Series("deficit_power_kw", metrics["deficit_power_kw"]),
        pl.Series("battery_opening_kw_min", metrics["battery_opening_kw_min"]),
        pl.Series("battery_closing_kw_min", metrics["battery_closing_kw_min"]),
        pl.Series("battery_draw_required_kw", metrics["battery_draw_required_kw"]),
        pl.Series("battery_draw_c_rate", metrics["battery_draw_c_rate"]),
        pl.Series("battery_draw_loss_rate", metrics["battery_draw_loss_rate"]),
        pl.Series("battery_draw_loss_kw", metrics["battery_draw_loss_kw"]),
        pl.Series("battery_draw_final_kw", metrics["battery_draw_final_kw"]),
        pl.Series("battery_draw_cumulative_kw_min", metrics["battery_draw_cumulative_kw_min"]),
        pl.Series("grid_buy_kw", metrics["grid_buy_kw"]),
        pl.Series("battery_store_available_kw", metrics["battery_store_available_kw"]),
        pl.Series("battery_store_c_rate", metrics["battery_store_c_rate"]),
        pl.Series("battery_store_loss_rate", metrics["battery_store_loss_rate"]),
        pl.Series("battery_store_loss_kw", metrics["battery_store_loss_kw"]),
        pl.Series("battery_store_final_kw", metrics["battery_store_final_kw"]),
        pl.Series("battery_store_cumulative_kw_min", metrics["battery_store_cumulative_kw_min"]),
        pl.Series("grid_sell_kw", metrics["grid_sell_kw"]),
        pl.Series("soc_kw_min", metrics["soc_kw_min"]),
        pl.Series("soc_fraction", metrics["soc_fraction"]),
        pl.Series("soc_pct", metrics["soc_pct"]),
        pl.Series("discharge_cycle_count", metrics["discharge_cycle_count"]),
        pl.Series("charge_cycle_count", metrics["charge_cycle_count"]),
        pl.Series("cum_charge_count", metrics["cum_charge_count"]),
        pl.Series("energy_sources_kw", metrics["energy_sources_kw"]),
        pl.Series("energy_uses_kw", metrics["energy_uses_kw"]),
        pl.Series("energy_losses_kw", metrics["energy_losses_kw"]),
        pl.Series("identity_1_error_kw", metrics["identity_1_error_kw"]),
        pl.Series("identity_1_ok", metrics["identity_1_ok"]),
        pl.Series("bess_start_kw_min", metrics["bess_start_kw_min"]),
        pl.Series("bess_discharge_kw", metrics["bess_discharge_kw"]),
        pl.Series("bess_discharge_loss_kw", metrics["bess_discharge_loss_kw"]),
        pl.Series("bess_charge_kw", metrics["bess_charge_kw"]),
        pl.Series("bess_charge_loss_kw", metrics["bess_charge_loss_kw"]),
        pl.Series("bess_finish_kw_min", metrics["bess_finish_kw_min"]),
        pl.Series("identity_2_error_kw_min", metrics["identity_2_error_kw_min"]),
        pl.Series("identity_2_ok", metrics["identity_2_ok"]),
    )
    context.validate_balance(result)
    return result


# Chunk size for writing section CSVs to limit peak memory (float64: ~50k rows ≈ tens of MB per chunk)
SECTION_WRITE_CHUNK_ROWS = 50_000


def write_section_outputs(
    df: pl.DataFrame,
    target_dir: Path,
    progress_callback: Callable[[str, float, str], None] | None = None,
    progress_pct_start: float = 94.0,
    progress_pct_end: float = 99.0,
) -> list[Path]:
    """Write one CSV per section, in row chunks to keep memory low (float64-friendly)."""
    target_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    n_rows = df.height

    # Generic base columns we want visible on all exported sections
    base_columns = ["timestamp", "wind_kw", "solar_kw", "total_generation_kw"]

    for i, section in enumerate(OUTPUT_SECTIONS):
        explicit_columns = [col for col in section.columns if col not in base_columns]
        target_columns = base_columns + explicit_columns
        available = [c for c in target_columns if c in df.columns]
        if not available:
            continue

        if progress_callback:
            n_total = len(OUTPUT_SECTIONS)
            pct = progress_pct_start + (progress_pct_end - progress_pct_start) * ((i + 1) / n_total)
            progress_callback("Writing sections", pct, f"Writing {section.file_name}")

        file_path = target_dir / section.file_name
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv_module.writer(f)
            writer.writerow(available)
            for start in range(0, n_rows, SECTION_WRITE_CHUNK_ROWS):
                chunk = df.slice(start, SECTION_WRITE_CHUNK_ROWS).select(available)
                for row in chunk.iter_rows():
                    writer.writerow(row)
        written_paths.append(file_path)
        gc.collect()

    return written_paths


def _simulate_section_accounting(
    total_generation: np.ndarray,
    total_consumption: np.ndarray,
    wind: np.ndarray,
    solar: np.ndarray,
    config: BatteryConfig,
    *,
    dtype: str = "float32",
    identity_tolerance: float = IDENTITY_TOLERANCE_FLOAT32,
    progress_callback: Callable[[str, float, str], None] | None = None,
) -> dict[str, np.ndarray]:
    row_count = total_generation.shape[0]
    np_dtype = np.float32 if dtype == "float32" else np.float64

    cum_wind = np.cumsum(wind.astype(np_dtype))
    cum_solar = np.cumsum(solar.astype(np_dtype))
    cum_total = np.cumsum(total_generation.astype(np_dtype))

    current_cycle = np.zeros(row_count, dtype=np_dtype)
    cumulative_degradation = np.zeros(row_count, dtype=np_dtype)
    capacity_now_kw_min = np.zeros(row_count, dtype=np_dtype)
    excess_power_kw = np.zeros(row_count, dtype=np_dtype)
    deficit_power_kw = np.zeros(row_count, dtype=np_dtype)
    battery_opening_kw_min = np.zeros(row_count, dtype=np_dtype)
    battery_closing_kw_min = np.zeros(row_count, dtype=np_dtype)
    battery_draw_required_kw = np.zeros(row_count, dtype=np_dtype)
    battery_draw_c_rate = np.zeros(row_count, dtype=np_dtype)
    battery_draw_loss_rate = np.zeros(row_count, dtype=np_dtype)
    battery_draw_loss_kw = np.zeros(row_count, dtype=np_dtype)
    battery_draw_final_kw = np.zeros(row_count, dtype=np_dtype)
    battery_draw_cumulative_kw_min = np.zeros(row_count, dtype=np_dtype)
    grid_buy_kw = np.zeros(row_count, dtype=np_dtype)
    battery_store_available_kw = np.zeros(row_count, dtype=np_dtype)
    battery_store_c_rate = np.zeros(row_count, dtype=np_dtype)
    battery_store_loss_rate = np.zeros(row_count, dtype=np_dtype)
    battery_store_loss_kw = np.zeros(row_count, dtype=np_dtype)
    battery_store_final_kw = np.zeros(row_count, dtype=np_dtype)
    battery_store_cumulative_kw_min = np.zeros(row_count, dtype=np_dtype)
    grid_sell_kw = np.zeros(row_count, dtype=np_dtype)
    soc_kw_min = np.zeros(row_count, dtype=np_dtype)
    soc_fraction = np.zeros(row_count, dtype=np_dtype)
    soc_pct = np.zeros(row_count, dtype=np_dtype)
    discharge_cycle_count = np.zeros(row_count, dtype=np_dtype)
    charge_cycle_count = np.zeros(row_count, dtype=np_dtype)
    cum_charge_count = np.zeros(row_count, dtype=np_dtype)
    energy_sources_kw = np.zeros(row_count, dtype=np_dtype)
    energy_uses_kw = np.zeros(row_count, dtype=np_dtype)
    energy_losses_kw = np.zeros(row_count, dtype=np_dtype)
    identity_1_error_kw = np.zeros(row_count, dtype=np_dtype)
    identity_1_ok = np.zeros(row_count, dtype=np.int8)
    bess_start_kw_min = np.zeros(row_count, dtype=np_dtype)
    bess_discharge_kw = np.zeros(row_count, dtype=np_dtype)
    bess_discharge_loss_kw = np.zeros(row_count, dtype=np_dtype)
    bess_charge_kw = np.zeros(row_count, dtype=np_dtype)
    bess_charge_loss_kw = np.zeros(row_count, dtype=np_dtype)
    bess_finish_kw_min = np.zeros(row_count, dtype=np_dtype)
    identity_2_error_kw_min = np.zeros(row_count, dtype=np_dtype)
    identity_2_ok = np.zeros(row_count, dtype=np.int8)

    nominal_capacity_kw_min = float(config.capacity_kwh) * 60.0
    nominal_capacity_kwh = float(config.capacity_kwh)
    prior_closing_kw_min = max(config.initial_soc_fraction, 0.0) * nominal_capacity_kw_min
    cumulative_drawn_kw_min = 0.0
    cumulative_stored_kw_min = 0.0
    prior_charge_count = 0.0

    progress_interval = max(1, row_count // 20)
    for index in range(row_count):
        if progress_callback and (index % progress_interval == 0 or index == row_count - 1):
            loop_pct = (index + 1) / row_count
            overall_pct = 15.0 + 75.0 * loop_pct
            progress_callback("Simulating", overall_pct, f"Minute {index + 1} of {row_count}")
        current_cycle[index] = prior_charge_count
        cumulative_degradation[index] = prior_charge_count * config.degradation_per_cycle
        capacity_now_kw_min[index] = max(nominal_capacity_kw_min * (1.0 - cumulative_degradation[index]), 0.0)

        # state in kWh bounded by capacity
        battery_opening_kw_min[index] = min(max(prior_closing_kw_min, 0.0), capacity_now_kw_min[index])

        excess = max(float(total_generation[index]) - float(total_consumption[index]), 0.0)
        deficit = max(float(total_consumption[index]) - float(total_generation[index]), 0.0)
        excess_power_kw[index] = excess
        deficit_power_kw[index] = deficit

        # available power (kW) we can practically draw based on stored minimum energy
        available_discharge_kw = battery_opening_kw_min[index]

        # discharge capped by state of charge
        required_draw = min(available_discharge_kw, deficit)
        battery_draw_required_kw[index] = required_draw

        # calculate loss on the drawn amount
        battery_draw_c_rate[index] = _rounded_c_rate(required_draw, nominal_capacity_kwh)
        battery_draw_loss_rate[index] = _lookup_loss_rate(battery_draw_c_rate[index], config.discharge_loss_table)
        battery_draw_loss_kw[index] = battery_draw_loss_rate[index] * required_draw

        # total drawn from battery is required + loss, capped by what's actually there
        draw_total = required_draw + battery_draw_loss_kw[index]
        if draw_total > available_discharge_kw:
            # Cap to available: we can only extract total battery energy.
            # Actually, to be perfectly physically consistent: we can only extract total battery energy.
            # Thus battery_draw_final_kw is minimum of required+loss vs available
            battery_draw_final_kw[index] = available_discharge_kw
            # which means required draw actually satisfied is available - loss roughly.
            # We'll just cap it simply:
            battery_draw_loss_kw[index] = battery_draw_loss_rate[index] * (available_discharge_kw / (1 + battery_draw_loss_rate[index]))
            battery_draw_required_kw[index] = battery_draw_final_kw[index] - battery_draw_loss_kw[index]
        else:
            battery_draw_final_kw[index] = draw_total

        cumulative_drawn_kw_min += battery_draw_final_kw[index]
        battery_draw_cumulative_kw_min[index] = cumulative_drawn_kw_min

        remaining_headroom_kw = max(capacity_now_kw_min[index] - battery_opening_kw_min[index], 0.0)
        store_available = 0.0
        if excess > 0.0 and remaining_headroom_kw > 0.0:
            # charge strictly proportional to remaining headroom
            store_available = min(excess, remaining_headroom_kw)

        battery_store_available_kw[index] = store_available
        battery_store_c_rate[index] = _rounded_c_rate(store_available, nominal_capacity_kwh)
        battery_store_loss_rate[index] = _lookup_loss_rate(battery_store_c_rate[index], config.charge_loss_table)
        battery_store_loss_kw[index] = battery_store_loss_rate[index] * store_available

        # cap what goes in by remaining headroom
        # the net energy entering battery state is store_available - loss
        net_store = max(store_available - battery_store_loss_kw[index], 0.0)
        if net_store > remaining_headroom_kw:
            battery_store_final_kw[index] = remaining_headroom_kw
            # recalculate required inputs
            store_available = remaining_headroom_kw / (1 - battery_store_loss_rate[index])
            battery_store_loss_kw[index] = store_available - battery_store_final_kw[index]
            battery_store_available_kw[index] = store_available
        else:
            battery_store_final_kw[index] = net_store

        cumulative_stored_kw_min += battery_store_final_kw[index]
        battery_store_cumulative_kw_min[index] = cumulative_stored_kw_min

        # Recalculate grid flow now that we know exactly what battery did
        grid_buy_kw[index] = max(deficit - battery_draw_required_kw[index], 0.0)
        grid_sell_kw[index] = max(excess - battery_store_available_kw[index], 0.0)

        # closing state physics
        battery_closing_kw_min[index] = max(
            min(
                battery_opening_kw_min[index] - battery_draw_final_kw[index] + battery_store_final_kw[index],
                capacity_now_kw_min[index],
            ),
            0.0,
        )
        soc_kw_min[index] = battery_closing_kw_min[index]

        # normalized views
        if nominal_capacity_kw_min > 0:
            soc_fraction[index] = soc_kw_min[index] / nominal_capacity_kw_min
            discharge_cycle_count[index] = cumulative_drawn_kw_min / nominal_capacity_kw_min
            charge_cycle_count[index] = cumulative_stored_kw_min / nominal_capacity_kw_min

        soc_pct[index] = soc_fraction[index] * 100.0
        cum_charge_count[index] = discharge_cycle_count[index] + charge_cycle_count[index]

        # Validated energy identities
        energy_sources_kw[index] = total_generation[index] + battery_draw_final_kw[index] + grid_buy_kw[index]
        energy_uses_kw[index] = total_consumption[index] + battery_store_final_kw[index] + grid_sell_kw[index]
        energy_losses_kw[index] = battery_draw_loss_kw[index] + battery_store_loss_kw[index]

        identity_1_error_kw[index] = energy_sources_kw[index] - energy_uses_kw[index] - energy_losses_kw[index]
        identity_1_ok[index] = int(abs(identity_1_error_kw[index]) <= identity_tolerance)

        bess_start_kw_min[index] = battery_opening_kw_min[index]
        bess_discharge_kw[index] = battery_draw_required_kw[index]
        bess_discharge_loss_kw[index] = battery_draw_loss_kw[index]
        bess_charge_kw[index] = battery_store_available_kw[index]
        bess_charge_loss_kw[index] = battery_store_loss_kw[index]

        # BESS start/finish in kWh for identity 2
        bess_finish_kw_min[index] = (
            bess_start_kw_min[index]
            - bess_discharge_kw[index]
            - bess_discharge_loss_kw[index]
            + bess_charge_kw[index]
            - bess_charge_loss_kw[index]
        )
        identity_2_error_kw_min[index] = max(bess_finish_kw_min[index], 0.0) - battery_closing_kw_min[index]
        identity_2_ok[index] = int(abs(identity_2_error_kw_min[index]) <= identity_tolerance)

        prior_closing_kw_min = battery_closing_kw_min[index]
        prior_charge_count = cum_charge_count[index]

    return {
        "cum_wind": cum_wind,
        "cum_solar": cum_solar,
        "cum_total": cum_total,
        "current_cycle": current_cycle,
        "cumulative_degradation": cumulative_degradation,
        "capacity_now_kw_min": capacity_now_kw_min,
        "excess_power_kw": excess_power_kw,
        "deficit_power_kw": deficit_power_kw,
        "battery_opening_kw_min": battery_opening_kw_min,
        "battery_closing_kw_min": battery_closing_kw_min,
        "battery_draw_required_kw": battery_draw_required_kw,
        "battery_draw_c_rate": battery_draw_c_rate,
        "battery_draw_loss_rate": battery_draw_loss_rate,
        "battery_draw_loss_kw": battery_draw_loss_kw,
        "battery_draw_final_kw": battery_draw_final_kw,
        "battery_draw_cumulative_kw_min": battery_draw_cumulative_kw_min,
        "grid_buy_kw": grid_buy_kw,
        "battery_store_available_kw": battery_store_available_kw,
        "battery_store_c_rate": battery_store_c_rate,
        "battery_store_loss_rate": battery_store_loss_rate,
        "battery_store_loss_kw": battery_store_loss_kw,
        "battery_store_final_kw": battery_store_final_kw,
        "battery_store_cumulative_kw_min": battery_store_cumulative_kw_min,
        "grid_sell_kw": grid_sell_kw,
        "soc_kw_min": soc_kw_min,
        "soc_fraction": soc_fraction,
        "soc_pct": soc_pct,
        "discharge_cycle_count": discharge_cycle_count,
        "charge_cycle_count": charge_cycle_count,
        "cum_charge_count": cum_charge_count,
        "energy_sources_kw": energy_sources_kw,
        "energy_uses_kw": energy_uses_kw,
        "energy_losses_kw": energy_losses_kw,
        "identity_1_error_kw": identity_1_error_kw,
        "identity_1_ok": identity_1_ok,
        "bess_start_kw_min": bess_start_kw_min,
        "bess_discharge_kw": bess_discharge_kw,
        "bess_discharge_loss_kw": bess_discharge_loss_kw,
        "bess_charge_kw": bess_charge_kw,
        "bess_charge_loss_kw": bess_charge_loss_kw,
        "bess_finish_kw_min": bess_finish_kw_min,
        "identity_2_error_kw_min": identity_2_error_kw_min,
        "identity_2_ok": identity_2_ok,
    }


def _rounded_c_rate(power_kw: float, nominal_capacity_kwh: float) -> float:
    if nominal_capacity_kwh <= 0 or power_kw <= 0:
        return 0.0
    return round(power_kw / nominal_capacity_kwh, 1)


def _lookup_loss_rate(c_rate: float, loss_table: dict[float, float]) -> float:
    if not loss_table:
        return 0.0

    c_rate = float(c_rate)
    sorted_keys = sorted(loss_table.keys())

    if c_rate <= sorted_keys[0]:
        return float(loss_table[sorted_keys[0]])
    if c_rate >= sorted_keys[-1]:
        return float(loss_table[sorted_keys[-1]])

    idx = bisect.bisect_left(sorted_keys, c_rate)

    # We are exactly on a key
    if sorted_keys[idx] == c_rate:
        return float(loss_table[sorted_keys[idx]])

    # We are explicitly between two keys
    lower_k = sorted_keys[idx - 1]
    upper_k = sorted_keys[idx]

    lower_v = loss_table[lower_k]
    upper_v = loss_table[upper_k]

    # linear interpolation formula: y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    fraction = (c_rate - lower_k) / (upper_k - lower_k)
    return float(lower_v + fraction * (upper_v - lower_v))


def _identity_tolerance(dtype: str) -> float:
    return IDENTITY_TOLERANCE_FLOAT32 if dtype == "float32" else IDENTITY_TOLERANCE_FLOAT64
