"""CLI entrypoints for the SECI FDRE-V BESS model."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from seci_fdre_v_model.aligned_energy_report import print_aligned_energy_report, print_aligned_energy_report_with_suggestions
from seci_fdre_v_model.config import ProjectConfig
from seci_fdre_v_model.ideal_year_profiles import write_tiled_year_profiles
from seci_fdre_v_model.runner import run_full_study
from seci_fdre_v_model.tender_inputs import generate_tender_input_files


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SECI FDRE-V BESS model")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser(
        "generate-input-files",
        help="Generate tender-derived output profile and aux power files.",
    )
    generate_parser.add_argument("--config", required=True, help="Path to the project YAML configuration file.")

    run_parser = subparsers.add_parser(
        "run",
        help="Run the base simulation and all configured sensitivity cases.",
    )
    run_parser.add_argument("--config", required=True, help="Path to the project YAML configuration file.")
    run_parser.add_argument("--dump-sections", action="store_true", help="Write aligned inputs and section CSVs for the base case.")
    run_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity.",
    )

    report_parser = subparsers.add_parser(
        "report-aligned-energy",
        help="Print annual kWh totals on the aligned solar/wind/load minute table (before BESS).",
    )
    report_parser.add_argument("--config", required=True, help="Path to the project YAML configuration file.")
    report_parser.add_argument(
        "--suggest",
        action="store_true",
        help="Print heuristic scale suggestions (annual kWh) after the totals.",
    )
    report_parser.add_argument(
        "--excess-fraction",
        type=float,
        default=0.08,
        help="Target annual surplus fraction for suggestions (default 0.08).",
    )

    tile_parser = subparsers.add_parser(
        "build-ideal-year-profiles",
        help="Tile one-day solar/wind CSVs across the simulation year (see config/project.ideal_1mw.yaml).",
    )
    tile_parser.add_argument(
        "--config",
        help="Project YAML to read simulation_start / simulation_end and default --solar-source / --wind-source.",
    )
    tile_parser.add_argument(
        "--solar-source",
        help="One-day solar CSV (timestamp, Power in KW). Defaults from --config inputs when --config is set.",
    )
    tile_parser.add_argument(
        "--wind-source",
        help="One-day wind CSV (time stamp, Power in KW). Defaults from --config inputs when --config is set.",
    )
    tile_parser.add_argument(
        "--solar-out",
        default="data/ideal/generated_solar_2025.csv",
        help="Output CSV path for full-year solar (repo-root relative ok).",
    )
    tile_parser.add_argument(
        "--wind-out",
        default="data/ideal/generated_wind_2025.csv",
        help="Output CSV path for full-year wind.",
    )
    tile_parser.add_argument("--solar-scale", type=float, default=1.0, help="Multiply tiled solar kW.")
    tile_parser.add_argument("--wind-scale", type=float, default=1.0, help="Multiply tiled wind kW.")

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, getattr(args, "log_level", "INFO")),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.command == "report-aligned-energy":
        project = ProjectConfig.from_yaml(args.config)
        if getattr(args, "suggest", False):
            print_aligned_energy_report_with_suggestions(
                project.simulation,
                excess_fraction=float(getattr(args, "excess_fraction", 0.08)),
            )
        else:
            print_aligned_energy_report(project.simulation)
        return 0

    if args.command == "build-ideal-year-profiles":
        if args.config:
            project = ProjectConfig.from_yaml(args.config)
            start, end = project.project.simulation_start, project.project.simulation_end
            solar_src = args.solar_source or str(project.inputs.solar_path)
            wind_src = args.wind_source or str(project.inputs.wind_path)
        else:
            if not args.solar_source or not args.wind_source:
                print("error: without --config, both --solar-source and --wind-source are required.", flush=True)
                return 2
            ref = Path(__file__).resolve().parents[2] / "config" / "project.yaml"
            project = ProjectConfig.from_yaml(ref)
            start, end = project.project.simulation_start, project.project.simulation_end
            solar_src = args.solar_source
            wind_src = args.wind_source

        solar_path = Path(solar_src).expanduser().resolve()
        wind_path = Path(wind_src).expanduser().resolve()
        solar_out = Path(args.solar_out).expanduser()
        wind_out = Path(args.wind_out).expanduser()
        written = write_tiled_year_profiles(
            simulation_start=start,
            simulation_end=end,
            solar_source=solar_path,
            wind_source=wind_path,
            solar_out=solar_out,
            wind_out=wind_out,
            solar_scale=float(args.solar_scale),
            wind_scale=float(args.wind_scale),
        )
        print(written[0])
        print(written[1])
        return 0

    config = ProjectConfig.from_yaml(args.config)
    if args.command == "generate-input-files":
        written = generate_tender_input_files(config)
        for path in written:
            print(path)
        return 0

    result = run_full_study(config, dump_sections=getattr(args, "dump_sections", False))
    print(f"Study package written to {result.package_dir}")
    print(f"Workbook written to {result.workbook_path}")
    return 0
