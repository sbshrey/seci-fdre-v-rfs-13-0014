"""Summarize annual energy on the aligned minute table (before BESS dispatch)."""

from __future__ import annotations

from dataclasses import dataclass, replace

import polars as pl

from seci_fdre_v_model.config import SimulationConfig
from seci_fdre_v_model.core.pipeline import load_aligned_inputs


@dataclass(frozen=True)
class AlignedEnergySummary:
    """Energy integrals over the aligned horizon (kW-min sums, kWh derived)."""

    minutes: int
    solar_kwh: float
    wind_kwh: float
    generation_kwh: float
    output_profile_kwh: float
    aux_kwh: float
    consumption_kwh: float
    net_generation_minus_load_kwh: float
    surplus_minutes: int
    deficit_minutes: int
    aux_note: str | None = None


def kw_min_sum_to_kwh(value_kw_min: float) -> float:
    return float(value_kw_min) / 60.0


def summarize_aligned_frame(df: pl.DataFrame) -> AlignedEnergySummary:
    """Compute totals from an aligned minute dataframe (requires standard column names)."""
    required = (
        "solar_kw",
        "wind_kw",
        "total_generation_kw",
        "output_profile_kw",
        "aux_consumption_kw",
        "total_consumption_kw",
    )
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Aligned frame is missing columns: {', '.join(missing)}")

    sums = df.select(
        pl.col("solar_kw").sum().alias("solar_kw_min"),
        pl.col("wind_kw").sum().alias("wind_kw_min"),
        pl.col("total_generation_kw").sum().alias("gen_kw_min"),
        pl.col("output_profile_kw").sum().alias("out_kw_min"),
        pl.col("aux_consumption_kw").sum().alias("aux_kw_min"),
        pl.col("total_consumption_kw").sum().alias("cons_kw_min"),
    ).row(0)

    solar_kwh = kw_min_sum_to_kwh(sums[0])
    wind_kwh = kw_min_sum_to_kwh(sums[1])
    gen_kwh = kw_min_sum_to_kwh(sums[2])
    out_kwh = kw_min_sum_to_kwh(sums[3])
    aux_kwh = kw_min_sum_to_kwh(sums[4])
    cons_kwh = kw_min_sum_to_kwh(sums[5])

    surplus = int(
        df.filter(pl.col("total_generation_kw") > pl.col("total_consumption_kw")).height
    )
    deficit = int(
        df.filter(pl.col("total_generation_kw") < pl.col("total_consumption_kw")).height
    )

    return AlignedEnergySummary(
        minutes=df.height,
        solar_kwh=solar_kwh,
        wind_kwh=wind_kwh,
        generation_kwh=gen_kwh,
        output_profile_kwh=out_kwh,
        aux_kwh=aux_kwh,
        consumption_kwh=cons_kwh,
        net_generation_minus_load_kwh=gen_kwh - cons_kwh,
        surplus_minutes=surplus,
        deficit_minutes=deficit,
    )


def summarize_aligned_inputs(config: SimulationConfig) -> AlignedEnergySummary:
    """Load solar/wind/profile/aux, align to the simulation grid, and summarize."""
    aligned, _context = load_aligned_inputs(config)
    summary = summarize_aligned_frame(aligned)
    if config.load.uses_battery_state_aux:
        return replace(summary, aux_note="Idle-state approximation for battery_state aux mode.")
    return summary


def format_aligned_energy_report(summary: AlignedEnergySummary, *, plant_name: str) -> str:
    lines = [
        f"Plant: {plant_name}",
        f"Aligned horizon: {summary.minutes:,} minutes",
        "",
        "Annualised energy (column sums / 60 -> kWh):",
        f"  Solar:              {summary.solar_kwh:,.0f} kWh",
        f"  Wind:               {summary.wind_kwh:,.0f} kWh",
        f"  Solar + wind:       {summary.generation_kwh:,.0f} kWh",
        f"  Output profile:     {summary.output_profile_kwh:,.0f} kWh",
        f"  Aux:                {summary.aux_kwh:,.0f} kWh",
        f"  Total consumption:  {summary.consumption_kwh:,.0f} kWh",
        f"  Net (gen - load):   {summary.net_generation_minus_load_kwh:,.0f} kWh",
    ]
    if summary.aux_note:
        lines.append(f"  Aux basis:          {summary.aux_note}")
    lines.extend(
        [
            "",
            "Minute balance (before battery):",
            f"  Minutes with surplus (gen > load): {summary.surplus_minutes:,}",
            f"  Minutes with deficit (gen < load): {summary.deficit_minutes:,}",
        ]
    )
    return "\n".join(lines) + "\n"


def print_aligned_energy_report(config: SimulationConfig) -> None:
    summary = summarize_aligned_inputs(config)
    print(format_aligned_energy_report(summary, plant_name=config.plant_name), end="")


def suggest_alignment_scales(
    summary: AlignedEnergySummary,
    *,
    solar_multiplier: float = 1.0,
    wind_multiplier: float = 1.0,
    profile_multiplier: float = 1.0,
    excess_fraction: float = 0.08,
    renewable_scale_cap: float = 500.0,
    profile_scale_floor: float = 1e-6,
) -> dict[str, float | bool | str]:
    """
    Heuristic scale factors so annual solar+wind energy can approach annual load (with a small surplus target).

    Uses **totals only** (kWh); minute-by-minute shape still matters for real surplus and BESS cycling.
    Two equivalent levers (first-order): scale solar+wind together, or scale ``profile_multiplier`` (output load).
    """
    gen = max(float(summary.generation_kwh), 1e-18)
    cons = max(float(summary.consumption_kwh), 1e-18)
    xf = max(float(excess_fraction), 0.0)
    target_gen = cons * (1.0 + xf)

    k_raise_re_raw = target_gen / gen
    k_raise_re = min(k_raise_re_raw, float(renewable_scale_cap))
    re_cap_hit = k_raise_re_raw > float(renewable_scale_cap)

    k_lower_profile_raw = gen / target_gen
    if k_lower_profile_raw >= 1.0:
        k_lower_profile = 1.0
    else:
        k_lower_profile = max(float(k_lower_profile_raw), float(profile_scale_floor))

    notes: list[str] = []
    if re_cap_hit:
        notes.append(
            f"Renewable scale hit cap ({renewable_scale_cap:g}x); combine with lowering profile_multiplier."
        )
    if summary.surplus_minutes > 0 and summary.generation_kwh >= cons * 0.99:
        notes.append("Annual generation already rivals load; minute surplus exists—tweak multipliers carefully.")
    elif summary.generation_kwh < cons * 0.05:
        notes.append("Very small annual generation vs load—tile full-year solar/wind or raise multipliers.")

    return {
        "annual_load_to_generation_ratio": cons / gen,
        "excess_fraction_target": xf,
        "uniform_renewable_scale": float(k_raise_re),
        "uniform_renewable_scale_raw": float(k_raise_re_raw),
        "renewable_scale_cap_hit": bool(re_cap_hit),
        "profile_multiplier_scale": float(k_lower_profile),
        "implied_next_solar_multiplier": float(solar_multiplier) * float(k_raise_re),
        "implied_next_wind_multiplier": float(wind_multiplier) * float(k_raise_re),
        "implied_next_profile_multiplier": float(profile_multiplier) * float(k_lower_profile),
        "notes": " ".join(notes) if notes else "",
    }


def print_aligned_energy_report_with_suggestions(config: SimulationConfig, *, excess_fraction: float = 0.08) -> None:
    summary = summarize_aligned_inputs(config)
    print(format_aligned_energy_report(summary, plant_name=config.plant_name), end="")
    sug = suggest_alignment_scales(
        summary,
        solar_multiplier=config.data.solar_multiplier,
        wind_multiplier=config.data.wind_multiplier,
        profile_multiplier=config.load.profile_multiplier,
        excess_fraction=excess_fraction,
    )
    print("Suggested scales (annual kWh heuristic, not minute-optimal):")
    for key in (
        "annual_load_to_generation_ratio",
        "uniform_renewable_scale",
        "profile_multiplier_scale",
        "implied_next_solar_multiplier",
        "implied_next_wind_multiplier",
        "implied_next_profile_multiplier",
    ):
        print(f"  {key}: {sug[key]}")
    if sug.get("notes"):
        print(f"  notes: {sug['notes']}")
