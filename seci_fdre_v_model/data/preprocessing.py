"""Timestamp alignment and gap filling for generation datasets."""

from __future__ import annotations

from datetime import datetime

import polars as pl

from seci_fdre_v_model.config import PreprocessingConfig


def align_generation_to_minute(
    solar: pl.DataFrame,
    wind: pl.DataFrame,
    config: PreprocessingConfig,
) -> pl.DataFrame:
    """Resample solar and wind to 1-minute resolution and align them."""
    solar_resampled = _resample_source(solar, "solar_kw", config)
    wind_resampled = _resample_source(wind, "wind_kw", config)

    if config.align_to_full_year:
        min_ts = min(solar_resampled["timestamp"].min(), wind_resampled["timestamp"].min())
        max_ts = max(solar_resampled["timestamp"].max(), wind_resampled["timestamp"].max())
        year = min_ts.year if hasattr(min_ts, "year") else max_ts.year
        start = datetime(year, 1, 1, 0, 0, 0)
        end = datetime(year, 12, 31, 23, 59, 0)
        timeline = _minute_timeline(start, end, config.frequency)
    else:
        min_ts = min(solar_resampled["timestamp"].min(), wind_resampled["timestamp"].min())
        max_ts = max(solar_resampled["timestamp"].max(), wind_resampled["timestamp"].max())
        timeline = _minute_timeline(min_ts, max_ts, config.frequency)

    combined = (
        timeline.join(solar_resampled, on="timestamp", how="left")
        .join(wind_resampled, on="timestamp", how="left")
        .with_columns(
            pl.col("solar_kw").fill_null(0.0),
            pl.col("wind_kw").fill_null(0.0),
            pl.col("solar_kw_raw").fill_null(0.0),
            pl.col("wind_kw_raw").fill_null(0.0),
        )
        .with_columns((pl.col("solar_kw") + pl.col("wind_kw")).alias("total_generation_kw"))
        .sort("timestamp")
    )
    return combined


def _resample_source(frame: pl.DataFrame, value_column: str, config: PreprocessingConfig) -> pl.DataFrame:
    """Upsample a source to 1-minute resolution with bounded interpolation."""
    min_ts = frame["timestamp"].min()
    max_ts = frame["timestamp"].max()
    timeline = _minute_timeline(min_ts, max_ts, config.frequency)

    joined = timeline.join(frame, on="timestamp", how="left").with_columns(
        pl.col(value_column).is_not_null().alias("observed"),
        pl.when(pl.col(value_column).is_not_null())
        .then(pl.col("timestamp"))
        .otherwise(None)
        .alias("observed_ts"),
    )

    span_expr = (
        (pl.col("next_observed_ts") - pl.col("prev_observed_ts")).dt.total_minutes()
    ).alias("gap_span_minutes")

    prepared = (
        joined.with_columns(
            pl.col("observed_ts").forward_fill().alias("prev_observed_ts"),
            pl.col("observed_ts").backward_fill().alias("next_observed_ts"),
            pl.col(value_column).alias(f"{value_column}_raw"),
        )
        .with_columns(span_expr)
        .with_columns(
            pl.when(pl.col("observed"))
            .then(pl.col(value_column))
            .when(
                (pl.lit(config.gap_fill) == "zero")
            )
            .then(None) # Force gaps to stay null, downstream fill_null(0.0) covers this
            .when(
                (pl.col("gap_span_minutes").is_not_null())
                & (pl.col("gap_span_minutes") <= config.max_interpolation_gap_minutes + 1)
            )
            .then(pl.col(value_column).interpolate())
            .otherwise(None)
            .alias(f"{value_column}_interpolated")
        )
        .with_columns(
            pl.col(f"{value_column}_interpolated").fill_null(0.0).alias(value_column),
        )
        .select("timestamp", value_column, f"{value_column}_raw")
    )
    return prepared


def _minute_timeline(start: datetime, end: datetime, frequency: str) -> pl.DataFrame:
    """Create a minute-resolution timeline from start to end, inclusive."""
    if frequency != "1m":
        raise ValueError(f"Unsupported frequency: {frequency}")
    return pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start=start,
                end=end,
                interval="1m",
                eager=True,
            )
        }
    )
