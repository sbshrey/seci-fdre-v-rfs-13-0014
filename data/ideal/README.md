# Ideal 1 MW example profiles

Large minute-level CSVs are **not committed** here. Generate them locally:

```bash
python main.py build-ideal-year-profiles --config config/project.yaml
```

That tiles the one-day seed files from `config/project.yaml` inputs across `simulation_start` … `simulation_end`, writing:

- `generated_solar_2025.csv`
- `generated_wind_2025.csv`

Then use `config/project.ideal_1mw.yaml` as described in the comments at the top of that file (`generate-input-files`, `report-aligned-energy`, `run`).

Adjust `--solar-scale` / `--wind-scale` on the builder command if you want more or less renewable energy relative to the tender output profile.

If you run the **web** study on "Ideal 1 MW" but skip tiling, your workspace `inputs/solar.csv` / `wind.csv` may still be short samples: full-year alignment then pads with zeros, so the energy table shows huge **Draw from GRID** and minimal **Draw from BESS**. Use the web **Tile profiles** step (or this CLI) before expecting a balanced ideal study.
