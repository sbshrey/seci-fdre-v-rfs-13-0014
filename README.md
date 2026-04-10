# SECI FDRE-V BESS Model

Local SECI FDRE-V study runner with both a CLI workflow and a Docker Compose control-room web UI.

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

## Local Control Room

The repo now includes a single-user local web application that manages a persistent workspace, uploads and downloads active input files, runs the full study into immutable run directories, and visualizes outputs as tables and charts.

Start it directly:

```bash
source .venv/bin/activate
seci-fdre-v-web --host 127.0.0.1 --port 5000
```

Or with Docker Compose:

```bash
docker compose up --build
```

The app uses `SECI_FDRE_V_WORKSPACE` when set. By default it creates a local `.workspace/` directory with:

- `config/project.yaml`
- `inputs/`
- `runs/<run_id>/`

Each run stores:

- a config snapshot,
- the exact input files used,
- a `package/` directory with summaries, tables, parquet output, workbook, and zip archive.

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

The control-room workflow writes immutable run packages to `.workspace/runs/<run_id>/package/` instead of overwriting prior runs.
