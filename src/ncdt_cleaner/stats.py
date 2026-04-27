'''
File description:
Small statistics and CSV-table helpers used across the pipeline.

These helpers keep summary generation uniform so cleaning, characterization, and
benchmark outputs are easy to compare and export into reports.
'''

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from .models import SensorDataset


def dataset_summary(dataset: SensorDataset) -> dict:
    """Return a compact summary dictionary for a normalized dataset."""
    return {
        "dataset_name": dataset.name,
        "n_rows": dataset.n_rows(),
        "n_sensors": len(dataset.sensors),
        "sensor_names": dataset.sensor_names(),
        "time_min": float(np.nanmin(dataset.time)) if dataset.n_rows() else None,
        "time_max": float(np.nanmax(dataset.time)) if dataset.n_rows() else None,
    }


def write_csv_table(path: str | Path, rows: list[dict]) -> None:
    """Write a list of dictionaries to a CSV table."""
    import csv
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
