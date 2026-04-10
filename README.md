# SECI FDRE-V BESS Model

CLI-first Python implementation of the SECI FDRE-V BESS model in this repo.

## What It Does

- generates separate tender-derived files for:
  - output profile
  - output profile for `18:00-22:00`
  - aux power
- runs a minute-level BESS simulation from solar, wind, output-profile, and aux-power files
- creates:
  - base summary
  - energy table
  - cases table
  - sensitivity cross table
  - Excel workbook and zip package

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python main.py generate-input-files --config config/project.yaml
python main.py run --config config/project.yaml
```

## Config

The starter config is [`config/project.yaml`](/Users/shreybaheti/Library/CloudStorage/OneDrive-CargillInc/Documents/ShreyBaheti/seci-fdre-v-rfs-13-0014/config/project.yaml).

Key sections:

- `project`: plant name, output dir, simulation window
- `inputs`: solar, wind, output profile, evening profile, aux power file paths
- `simulation`: data toggles, grid, load, battery
- `sensitivity`: wind, solar, profile, battery capacity, battery hours

## Outputs

Each run writes to `output/<plant_name>/`:

- `base_case_minute_flows.parquet`
- `base_case_summary.csv`
- `base_case_energy_table.csv`
- `energy_table.csv`
- `base_summary.csv`
- `cases_table.csv`
- `sensitivity_cross_table.csv`
- `profile_files_index.csv`
- `<plant_name>.xlsx`
- `<plant_name>.zip`
