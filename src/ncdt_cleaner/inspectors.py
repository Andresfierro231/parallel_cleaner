from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .readers import detect_csv_encoding, estimate_csv_rows, read_tabular_file
from .schema import infer_schema
from .xlsx_xml import list_sheets, quick_inspect

LOGGER = logging.getLogger(__name__)


def inspect_file(
    path: str | Path,
    config: dict,
    max_sample_rows: int = 5,
) -> dict[str, Any]:
    path = Path(path)
    info: dict[str, Any] = {
        "path": str(path),
        "name": path.name,
        "suffix": path.suffix.lower(),
        "size_bytes": path.stat().st_size,
    }
    suffix = path.suffix.lower()

    if suffix == ".csv":
        enc = detect_csv_encoding(path)
        df = read_tabular_file(path)
        schema = infer_schema(
            df,
            manual_time_column=config["schema"].get("manual_time_column"),
            manual_sensor_columns=config["schema"].get("manual_sensor_columns"),
            exclude_columns=config["schema"].get("exclude_columns", []),
        )
        info.update(
            {
                "encoding": enc,
                "estimated_rows": estimate_csv_rows(path),
                "columns": df.columns.tolist(),
                "sample_rows": df.head(max_sample_rows).to_dict(orient="records"),
                "schema_mapping": {
                    "original_headers": schema.original_headers,
                    "normalized_headers": schema.normalized_headers,
                    "time_column": {
                        "original_name": schema.time_column.original_name,
                        "normalized_name": schema.time_column.normalized_name,
                        "confidence": schema.time_column.confidence,
                        "candidate_scores": schema.time_column.candidate_scores,
                        "reasoning": schema.time_column.reasoning,
                        "ambiguous": schema.time_column.ambiguous,
                    },
                    "sensor_columns": schema.sensor_columns,
                    "notes": schema.notes,
                },
            }
        )
        return info

    if suffix == ".xlsx":
        sample = quick_inspect(path)
        df = read_tabular_file(path)
        schema = infer_schema(
            df,
            manual_time_column=config["schema"].get("manual_time_column"),
            manual_sensor_columns=config["schema"].get("manual_sensor_columns"),
            exclude_columns=config["schema"].get("exclude_columns", []),
        )
        info.update(
            {
                "sheets": list_sheets(path),
                "columns": df.columns.tolist(),
                "sample_rows": sample["sample_rows"],
                "shape": sample["shape"],
                "schema_mapping": {
                    "original_headers": schema.original_headers,
                    "normalized_headers": schema.normalized_headers,
                    "time_column": {
                        "original_name": schema.time_column.original_name,
                        "normalized_name": schema.time_column.normalized_name,
                        "confidence": schema.time_column.confidence,
                        "candidate_scores": schema.time_column.candidate_scores,
                        "reasoning": schema.time_column.reasoning,
                        "ambiguous": schema.time_column.ambiguous,
                    },
                    "sensor_columns": schema.sensor_columns,
                    "notes": schema.notes,
                },
            }
        )
        return info

    if suffix in {".json", ".ndjson", ".h5", ".hdf5"}:
        df = read_tabular_file(path)
        schema = infer_schema(
            df,
            manual_time_column=config["schema"].get("manual_time_column"),
            manual_sensor_columns=config["schema"].get("manual_sensor_columns"),
            exclude_columns=config["schema"].get("exclude_columns", []),
        )
        info.update(
            {
                "columns": df.columns.tolist(),
                "sample_rows": df.head(max_sample_rows).to_dict(orient="records"),
                "shape": [int(df.shape[0]), int(df.shape[1])],
                "schema_mapping": {
                    "original_headers": schema.original_headers,
                    "normalized_headers": schema.normalized_headers,
                    "time_column": {
                        "original_name": schema.time_column.original_name,
                        "normalized_name": schema.time_column.normalized_name,
                        "confidence": schema.time_column.confidence,
                        "candidate_scores": schema.time_column.candidate_scores,
                        "reasoning": schema.time_column.reasoning,
                        "ambiguous": schema.time_column.ambiguous,
                    },
                    "sensor_columns": schema.sensor_columns,
                    "notes": schema.notes,
                },
            }
        )
        return info

    raise ValueError(f"Unsupported file type for inspection: {path}")
