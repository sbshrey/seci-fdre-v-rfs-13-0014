"""CLI entrypoints for the SECI FDRE-V BESS model."""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

from seci_fdre_v_model.config import ProjectConfig
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

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, getattr(args, "log_level", "INFO")),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = ProjectConfig.from_yaml(args.config)
    if args.command == "generate-input-files":
        written = generate_tender_input_files(config)
        for path in written:
            print(path)
        return 0

    result = run_full_study(config, dump_sections=args.dump_sections)
    print(f"Study package written to {result.package_dir}")
    print(f"Workbook written to {result.workbook_path}")
    return 0
