'''
File description:
Binary cache read and write utilities for normalized sensor datasets.

The project converts raw CSV or XLSX input into a simple on-disk cache of NPY
files so later cleaning and benchmarking runs avoid repeated text parsing.
'''

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

from .models import SensorDataset
from .utils import ensure_dir, now_utc_iso, normalize_header

CACHE_METADATA_FILENAME = "cache_metadata.json"
CACHE_METADATA_VERSION = 1


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


def _path_signature(path: str | Path | None) -> dict[str, Any] | None:
    """Build a small signature for a file path used in cache matching."""
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    digest = hashlib.sha256()
    with open(resolved, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "resolved_path": str(resolved),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": digest.hexdigest(),
    }


def write_cache_metadata(
    cache_dir: str | Path,
    *,
    input_path: str | Path,
    config_path: str | Path | None,
    dataset_summary: dict[str, Any],
    sheet_name: str | None = None,
    session_dir: str | Path | None = None,
) -> Path:
    """Write top-level machine-readable cache provenance metadata."""
    cache_dir = Path(cache_dir).resolve()
    session_dir = Path(session_dir).resolve() if session_dir is not None else cache_dir.parent.resolve()
    metadata_path = session_dir / CACHE_METADATA_FILENAME
    metadata = {
        "version": CACHE_METADATA_VERSION,
        "created_utc": now_utc_iso(),
        "session_dir": str(session_dir),
        "cache_dir": str(cache_dir),
        "cache_metadata_path": str(metadata_path),
        "input": {
            "original_path": str(input_path),
            "sheet_name": sheet_name,
            "signature": _path_signature(input_path),
        },
        "config": {
            "path": str(config_path) if config_path is not None else None,
            "signature": _path_signature(config_path) if config_path is not None else None,
        },
        "dataset_summary": dataset_summary,
    }
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    return metadata_path


def load_cache_metadata(path: str | Path) -> dict[str, Any]:
    """Load one machine-readable cache metadata file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_matching_cache(
    analysis_dir: str | Path,
    *,
    input_path: str | Path,
    config_path: str | Path | None,
    sheet_name: str | None = None,
) -> dict[str, Any] | None:
    """Find an existing cache whose recorded provenance matches the input file."""
    analysis_dir = Path(analysis_dir).resolve()
    current_input_sig = _path_signature(input_path)
    current_config_sig = _path_signature(config_path) if config_path is not None else None

    candidates = sorted(analysis_dir.glob("*_session_*/cache_metadata.json"), reverse=True)
    for metadata_path in candidates:
        try:
            meta = load_cache_metadata(metadata_path)
        except Exception:
            continue

        cached_input = (meta.get("input") or {}).get("signature")
        cached_config = (meta.get("config") or {}).get("signature")
        if cached_input != current_input_sig:
            continue
        if (meta.get("input") or {}).get("sheet_name") != sheet_name:
            continue
        if current_config_sig != cached_config:
            continue

        cache_dir = Path(meta.get("cache_dir", "")).expanduser()
        if not cache_dir.is_absolute():
            cache_dir = (metadata_path.parent / cache_dir).resolve()
        if not (cache_dir / "metadata.json").exists():
            continue

        meta["cache_dir"] = str(cache_dir)
        meta["cache_metadata_path"] = str(metadata_path.resolve())
        return meta
    return None


def ensure_cache_for_input(
    *,
    input_path: str | Path,
    config_path: str | Path | None,
    config: dict[str, Any],
    analysis_dir: str | Path,
    sheet_name: str | None = None,
    target_cache_dir: str | Path | None = None,
    target_session_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Find a matching cache or build a new cache when none exists."""
    matched = find_matching_cache(
        analysis_dir,
        input_path=input_path,
        config_path=config_path,
        sheet_name=sheet_name,
    )
    if matched is not None:
        return {
            "status": "reused",
            "cache_dir": matched["cache_dir"],
            "cache_metadata_path": matched["cache_metadata_path"],
            "cache_metadata": matched,
            "summary": matched.get("dataset_summary"),
            "matched_session_dir": matched.get("session_dir"),
        }

    if target_cache_dir is None or target_session_dir is None:
        raise ValueError("Building a new cache requires target_cache_dir and target_session_dir")

    from .normalization import dataframe_to_sensor_dataset
    from .readers import read_tabular_file

    df = read_tabular_file(input_path, sheet_name=sheet_name)
    dataset, summary = dataframe_to_sensor_dataset(df, Path(input_path).stem, config)
    cache_dir = write_sensor_cache(dataset, target_cache_dir, dtype=config["cache"]["dtype"]).resolve()
    metadata_path = write_cache_metadata(
        cache_dir,
        input_path=input_path,
        config_path=config_path,
        dataset_summary=summary,
        sheet_name=sheet_name,
        session_dir=target_session_dir,
    )
    return {
        "status": "built",
        "cache_dir": str(cache_dir),
        "cache_metadata_path": str(metadata_path),
        "cache_metadata": load_cache_metadata(metadata_path),
        "summary": summary,
        "matched_session_dir": str(Path(target_session_dir).resolve()),
    }


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
