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
  - Excel summary workbook

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python main.py generate-input-files --config config/project.yaml
python main.py run --config config/project.yaml
```

### Balanced 1 MW example + alignment check

Use `config/project.ideal_1mw.yaml` for a **full-year tiled** solar/wind dataset (from the one-day seeds) and a **1 MW contracted** tender profile. Follow the numbered comments at the top of that YAML: tile profiles, `generate-input-files`, then optionally print **pre-BESS** annual energy parity:

```bash
python main.py build-ideal-year-profiles --config config/project.yaml
python main.py generate-input-files --config config/project.ideal_1mw.yaml
python main.py report-aligned-energy --config config/project.ideal_1mw.yaml
python main.py report-aligned-energy --config config/project.yaml --suggest --excess-fraction 0.08
python main.py run --config config/project.ideal_1mw.yaml
```

Tune `--solar-scale` / `--wind-scale` on `build-ideal-year-profiles`, or `simulation.data` multipliers / `simulation.load.profile_multiplier`, until `report-aligned-energy` shows the surplus/deficit mix you want for battery sizing.

## Local Control Room

The repo now includes a single-user local web application that manages a persistent workspace, uploads and downloads active input files, runs the full study into immutable run directories, and visualizes outputs as tables and charts.

Start it directly:

```bash
source .venv/bin/activate
seci-fdre-v-web --host 127.0.0.1 --port 8000
```

Or with Docker Compose:

```bash
docker compose up --build
```

The containerized path is the simplest replacement for the Windows `.exe` if endpoint security blocks packaged binaries. It runs the same localhost browser UI and persists study files in the repo-local `.workspace/` folder mounted into the container.

If port `8000` is already in use, override it when starting Compose:

```bash
SECI_FDRE_V_PORT=8050 docker compose up --build
```

Then open `http://127.0.0.1:8050`.

## Podman On macOS

`podman compose` on macOS still depends on an external compose provider such as `docker-compose` or `podman-compose`. If you see a "looking up compose provider failed" error, use the native Podman scripts in this repo instead:

```bash
podman machine start
sh scripts/podman-up.sh
```

That builds the same image from `Dockerfile`, starts the web app on `http://127.0.0.1:8000`, and mounts your repo-local `.workspace/` directory into the container.

To change the host port:

```bash
SECI_FDRE_V_PORT=8050 sh scripts/podman-up.sh
```

To stop and remove the container:

```bash
sh scripts/podman-down.sh
```

Windows desktop packaging is also supported for non-technical users. The packaged build launches the same localhost web UI, opens it in the default browser, and stays available through a tray icon until the user quits.

The app uses `SECI_FDRE_V_WORKSPACE` when set. By default it creates a local `.workspace/` directory with:

- `config/project.yaml`
- `inputs/`
- `runs/<run_id>/`

On the **Inputs** page, the **Ideal 1 MW workflow** panel mirrors the CLI ideal flow: apply the bundled `config/project.ideal_1mw.yaml` preset, tile `solar.csv` / `wind.csv` across the study horizon, regenerate tender profiles, and fetch a **pre-BESS alignment report** via `/api/aligned-energy-report`.

On **Run study**, the **Study config** dropdown chooses whether the run snapshot uses your workspace `project.yaml` or the bundled **Ideal 1 MW example** parameters (workspace CSV inputs either way); the choice is remembered for your browser session. On the **Config** page, changing that dropdown refreshes the form to match the selected profile (Save is disabled while previewing Ideal so you do not overwrite your workspace YAML by mistake).

Each run stores:

- a config snapshot,
- the exact input files used,
- a `package/` directory with summaries, tables, parquet output, and workbook.

## Config

The starter config is `config/project.yaml`.

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

The control-room workflow writes immutable run packages to `.workspace/runs/<run_id>/package/` instead of overwriting prior runs.

## Windows Portable Build

Build the Windows distribution on a Windows machine:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
.\release\windows\build-portable.ps1
```

That script installs the desktop-build dependencies, runs the web and desktop tests, builds a PyInstaller one-folder app, and creates `dist/SECI-FDRE-V-windows-portable.zip`. It first removes `dist/` and `build/`; if Explorer or a running `SECI-FDRE-V.exe` locks files, run `.\release\windows\clear-build-artifacts.ps1` alone (it retries with a robocopy empty-folder purge) or close those handles and retry.

For the end user:

1. unzip `SECI-FDRE-V-windows-portable.zip`
2. open the `SECI-FDRE-V` folder
3. double-click `SECI-FDRE-V.exe`
4. use the browser UI that opens automatically
5. quit from the system tray icon when finished

**Debugging the packaged app:** the GUI build has no console. Logs are appended to `%LOCALAPPDATA%\SECI FDRE V\control_room.log` (same folder as the default workspace). Run `SECI-FDRE-V.exe --console` to open a Windows console and mirror logs there as well. If a study appears stuck (for example near 40% during sensitivity cases), check that log file for tracebacks; parallel sensitivity work can take a long time on large configs.
