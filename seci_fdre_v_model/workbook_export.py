"""Workbook export helpers for the study package."""

from __future__ import annotations

from pathlib import Path

import polars as pl

PACKAGE_SHEETS: tuple[tuple[str, str], ...] = (
    ("Base Summary", "base_summary.csv"),
    ("Energy Table", "energy_table.csv"),
    ("Cases", "cases_table.csv"),
    ("Sensitivity Cross", "sensitivity_cross_table.csv"),
    ("Profile Files Index", "profile_files_index.csv"),
)


def write_summary_workbook(
    input_dir: str | Path,
    *,
    output: str | Path | None = None,
) -> Path:
    """Write the study summary ``.xlsx`` from the small CSVs listed in ``PACKAGE_SHEETS``."""
    package_dir = Path(input_dir).expanduser().resolve()
    if not package_dir.exists():
        raise FileNotFoundError(f"Study package directory not found: {package_dir}")
    workbook_path = _resolve_output_path(package_dir, output)
    frames = [(sheet_name, pl.read_csv(package_dir / filename)) for sheet_name, filename in PACKAGE_SHEETS]
    _write_workbook(frames, workbook_path)
    return workbook_path


def _resolve_output_path(package_dir: Path, output: str | Path | None) -> Path:
    output_path = package_dir / f"{package_dir.name}.xlsx" if output is None else (Path(output) if Path(output).is_absolute() else package_dir / Path(output))
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def _write_workbook(sheet_frames: list[tuple[str, pl.DataFrame]], workbook_path: Path) -> None:
    import xlsxwriter

    workbook = xlsxwriter.Workbook(str(workbook_path))
    try:
        for sheet_name, frame in sheet_frames:
            frame.write_excel(
                workbook=workbook,
                worksheet=sheet_name,
                freeze_panes="A2",
                autofilter=True,
                autofit=True,
                float_precision=2,
                column_formats=_column_formats(frame),
            )
    finally:
        workbook.close()


def _column_formats(frame: pl.DataFrame) -> dict[str, str] | None:
    formats: dict[str, str] = {}
    for column_name, dtype in frame.schema.items():
        if dtype.is_float():
            formats[column_name] = "#,##0.00"
        elif dtype.is_integer():
            formats[column_name] = "#,##0"
    return formats or None
