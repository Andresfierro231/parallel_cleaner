'''
File description:
Binary cache read and write utilities for normalized sensor datasets.

The project converts raw CSV or XLSX input into a simple on-disk cache of NPY
files so later cleaning and benchmarking runs avoid repeated text parsing.
'''

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from .models import SensorDataset
from .utils import ensure_dir, normalize_header


def write_sensor_cache(dataset: SensorDataset, cache_dir: str | Path, dtype: str = "float64") -> Path:
    """Write a normalized dataset into the project's cache directory layout."""
    cache_dir = ensure_dir(cache_dir)
    sensors_dir = ensure_dir(Path(cache_dir) / "sensors")
    time_path = Path(cache_dir) / "time.npy"
    np.save(time_path, dataset.time.astype(dtype))
    sensor_map: dict[str, str] = {}
    for sensor_name, values in dataset.sensors.items():
        # Cache filenames use normalized headers so they remain predictable and
        # filesystem-friendly across platforms.
        norm = normalize_header(sensor_name)
        sensor_path = sensors_dir / f"{norm}.npy"
        np.save(sensor_path, values.astype(dtype))
        sensor_map[sensor_name] = str(sensor_path)
    metadata = {
        "dataset_name": dataset.name,
        "n_rows": int(dataset.n_rows()),
        "sensor_names": list(dataset.sensors.keys()),
        "sensor_files": sensor_map,
        "time_file": str(time_path),
        "user_metadata": dataset.metadata,
    }
    with open(Path(cache_dir) / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    return Path(cache_dir)


def load_sensor_cache(cache_dir: str | Path, mmap_mode: str | None = "r") -> SensorDataset:
    """Load a cached dataset, optionally memory-mapping large arrays."""
    cache_dir = Path(cache_dir)
    with open(cache_dir / "metadata.json", "r", encoding="utf-8") as f:
        meta = json.load(f)
    time = np.load(cache_dir / "time.npy", mmap_mode=mmap_mode)
    sensors = {
        sensor_name: np.load(path, mmap_mode=mmap_mode)
        for sensor_name, path in meta["sensor_files"].items()
    }
    return SensorDataset(
        name=meta["dataset_name"],
        time=time,
        sensors=sensors,
        metadata=meta.get("user_metadata", {}),
    )
