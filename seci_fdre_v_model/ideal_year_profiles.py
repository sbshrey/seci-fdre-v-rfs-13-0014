"""Tile one-day solar/wind CSVs across a full simulation year for coherent annual studies."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl


def _minute_of_day_expr(ts: pl.Expr) -> pl.Expr:
    return (ts.dt.hour().cast(pl.Int32) * 60 + ts.dt.minute().cast(pl.Int32)).alias("minute_of_day")


def build_minute_lookup_from_solar_day(path: Path, *, solar_format: str = "%d/%m/%Y %H:%M") -> pl.DataFrame:
    raw = pl.read_csv(path)
    if "timestamp" not in raw.columns or "Power in KW" not in raw.columns:
        raise ValueError(f"Solar file {path} must have columns 'timestamp' and 'Power in KW'.")
    parsed = raw.select(
        pl.col("timestamp")
        .cast(pl.String)
        .str.strip_chars()
        .str.strptime(pl.Datetime, format=solar_format, strict=True)
        .alias("ts"),
        pl.col("Power in KW").cast(pl.Float64).alias("power_kw"),
    )
    return parsed.with_columns(_minute_of_day_expr(pl.col("ts"))).select("minute_of_day", "power_kw").unique(
        "minute_of_day", keep="first"
    )


def build_minute_lookup_from_wind_day(path: Path, *, wind_format: str = "%Y-%m-%d %H:%M") -> pl.DataFrame:
    raw = pl.read_csv(path)
    col_ts = "time stamp" if "time stamp" in raw.columns else "timestamp"
    if col_ts not in raw.columns or "Power in KW" not in raw.columns:
        raise ValueError(f"Wind file {path} must have 'time stamp' (or timestamp) and 'Power in KW'.")
    parsed = raw.select(
        pl.col(col_ts)
        .cast(pl.String)
        .str.strip_chars()
        .str.strptime(pl.Datetime, format=wind_format, strict=True)
        .alias("ts"),
        pl.col("Power in KW").cast(pl.Float64).alias("power_kw"),
    )
    return parsed.with_columns(_minute_of_day_expr(pl.col("ts"))).select("minute_of_day", "power_kw").unique(
        "minute_of_day", keep="first"
    )


def write_tiled_year_profiles(
    *,
    simulation_start: datetime,
    simulation_end: datetime,
    solar_source: Path,
    wind_source: Path,
    solar_out: Path,
    wind_out: Path,
    solar_scale: float = 1.0,
    wind_scale: float = 1.0,
) -> tuple[Path, Path]:
    """Write full-year minute CSVs compatible with `load_generation_data` column names and formats."""
    solar_lut = build_minute_lookup_from_solar_day(solar_source)
    wind_lut = build_minute_lookup_from_wind_day(wind_source)

    timeline = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start=simulation_start,
                end=simulation_end,
                interval="1m",
                eager=True,
            )
        }
    ).with_columns(_minute_of_day_expr(pl.col("timestamp")))

    solar_frame = (
        timeline.join(solar_lut, on="minute_of_day", how="left")
        .with_columns(pl.col("power_kw").fill_null(0.0).alias("solar_kw"))
        .with_columns((pl.col("solar_kw") * float(solar_scale)).alias("solar_kw"))
        .select(
            pl.col("timestamp").dt.strftime("%d/%m/%Y %H:%M").alias("timestamp"),
            pl.col("solar_kw").alias("Power in KW"),
        )
    )
    wind_frame = (
        timeline.join(wind_lut, on="minute_of_day", how="left")
        .with_columns(pl.col("power_kw").fill_null(0.0).alias("wind_kw"))
        .with_columns((pl.col("wind_kw") * float(wind_scale)).alias("wind_kw"))
        .select(
            pl.col("timestamp").dt.strftime("%Y-%m-%d %H:%M").alias("time stamp"),
            pl.col("wind_kw").alias("Power in KW"),
        )
    )

    solar_out.parent.mkdir(parents=True, exist_ok=True)
    wind_out.parent.mkdir(parents=True, exist_ok=True)
    solar_frame.write_csv(solar_out)
    wind_frame.write_csv(wind_out)
    return solar_out, wind_out
