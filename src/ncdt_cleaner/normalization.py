'''
File description:
Normalization logic that converts heterogeneous tables into a common dataset.

This stage bridges raw `pandas` DataFrames and the internal cache format by
choosing the time column, selecting numeric sensors, and producing arrays.
'''

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .models import SensorDataset
from .schema import infer_schema

LOGGER = logging.getLogger(__name__)


def dataframe_to_sensor_dataset(
    df: pd.DataFrame,
    dataset_name: str,
    config: dict,
) -> tuple[SensorDataset, dict]:
    """Convert a raw DataFrame into the project's normalized dataset object."""
    schema = infer_schema(
        df,
        manual_time_column=config["schema"].get("manual_time_column"),
        manual_sensor_columns=config["schema"].get("manual_sensor_columns"),
        exclude_columns=config["schema"].get("exclude_columns", []),
    )

    time_col = schema.time_column.original_name
    if time_col is None or schema.time_column.ambiguous:
        if config.get("allow_index_time_fallback", False):
            time = np.arange(len(df), dtype=float)
            time_source = "generated_index"
        else:
            raise ValueError("Could not confidently infer time column; pass a manual override")
    else:
        raw_time = df[time_col]
        numeric_time = pd.to_numeric(raw_time, errors="coerce")
        if numeric_time.notna().mean() > 0.9:
            time = numeric_time.to_numpy(dtype=float)
            time_source = time_col
        else:
            parsed = pd.to_datetime(raw_time, errors="coerce")
            if parsed.notna().mean() > 0.9:
                # Convert timestamps into elapsed seconds so later numerical
                # stages can treat time as a plain floating-point array.
                time = (parsed - parsed.iloc[0]).dt.total_seconds().to_numpy(dtype=float)
                time_source = time_col
            elif config.get("allow_index_time_fallback", False):
                time = np.arange(len(df), dtype=float)
                time_source = "generated_index"
            else:
                raise ValueError(f"Time column {time_col} is not cleanly numeric/datetime")

    sensors: dict[str, np.ndarray] = {}
    for col in schema.sensor_columns:
        # All sensor channels are stored as float arrays so downstream code can
        # use one predictable numeric representation.
        values = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=float)
        sensors[col] = values

    dataset = SensorDataset(
        name=dataset_name,
        time=time,
        sensors=sensors,
        metadata={
            "schema_mapping": {
                "time_column": {
                    "original_name": schema.time_column.original_name,
                    "normalized_name": schema.time_column.normalized_name,
                    "confidence": schema.time_column.confidence,
                    "candidate_scores": schema.time_column.candidate_scores,
                    "ambiguous": schema.time_column.ambiguous,
                },
                "sensor_columns": schema.sensor_columns,
                "normalized_headers": schema.normalized_headers,
                "notes": schema.notes,
            },
            "time_source": time_source,
        },
    )
    summary = {
        "dataset_name": dataset_name,
        "n_rows": int(len(time)),
        "n_sensors": len(sensors),
        "sensor_columns": list(sensors.keys()),
        "time_source": time_source,
        "schema_mapping": dataset.metadata["schema_mapping"],
    }
    return dataset, summary
