'''
File description:
MPI execution strategies for replicated-data and partitioned-time cleaning.

This module contains the two parallel execution modes discussed in the report:
one simple replicated baseline and one partitioned approach with halo overlap.
'''

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
from ._mpi import MPI, ensure_mpi_initialized

from .cache import load_sensor_cache
from .characterize import characterize_signal
from .cleaning import clean_sensor
from .stats import write_csv_table
from .utils import ensure_dir, normalize_header

LOGGER = logging.getLogger(__name__)


def _rank_chunk(items: list[str], rank: int, size: int) -> list[str]:
    """Assign items to ranks using a simple round-robin pattern."""
    return [item for i, item in enumerate(items) if i % size == rank]


def run_replicated_mode(cache_dir: str | Path, output_dir: str | Path, cleaning_cfg: dict, char_cfg: dict, do_characterize: bool = False) -> dict:
    """Run the replicated-data MPI strategy described in the paper."""
    ensure_mpi_initialized()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    dataset = load_sensor_cache(cache_dir, mmap_mode="r")
    sensor_names = dataset.sensor_names()
    local_sensors = _rank_chunk(sensor_names, rank, size)

    local_payload = {}
    for sensor in local_sensors:
        # Every rank reads the same cache, but only processes its assigned
        # subset of sensors so the work is distributed.
        result = clean_sensor(np.asarray(dataset.sensors[sensor], dtype=float), cleaning_cfg)
        payload = {
            "cleaned": result.cleaned,
            "flags": result.flags.astype(np.uint8),
            "stats": result.stats,
        }
        if do_characterize:
            payload["characterization"] = characterize_signal(dataset.time, result.cleaned, **char_cfg)
        local_payload[sensor] = payload

    gathered = comm.gather(local_payload, root=0)
    summary = {"mode": "replicated", "nproc": size, "rank": rank}
    if rank == 0:
        # Rank 0 merges payloads and performs all file writes for the mode.
        merged = {}
        for part in gathered:
            merged.update(part)
        out_dir = ensure_dir(output_dir)
        sensors_dir = ensure_dir(Path(out_dir) / "sensors")
        summary_rows = []
        for sensor, payload in merged.items():
            np.save(Path(sensors_dir) / f"{normalize_header(sensor)}_cleaned.npy", payload["cleaned"])
            np.save(Path(sensors_dir) / f"{normalize_header(sensor)}_flags.npy", payload["flags"])
            if "characterization" in payload:
                with open(Path(sensors_dir) / f"{normalize_header(sensor)}_characterization.json", "w", encoding="utf-8") as f:
                    json.dump(payload["characterization"], f)
            summary_rows.append({"sensor": sensor, **payload["stats"]})
        write_csv_table(Path(out_dir) / "cleaning_summary.csv", summary_rows)
        summary["written_sensors"] = len(summary_rows)
    return summary


def run_partitioned_mode(cache_dir: str | Path, output_dir: str | Path, cleaning_cfg: dict) -> dict:
    """Run the partitioned time-domain MPI strategy with halo overlap."""
    ensure_mpi_initialized()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    dataset = load_sensor_cache(cache_dir, mmap_mode="r")
    n = dataset.n_rows()
    window = int(cleaning_cfg["window_radius"])

    start = (rank * n) // size
    end = ((rank + 1) * n) // size
    halo_start = max(0, start - window)
    halo_end = min(n, end + window)

    local_payload = {}
    for sensor, values in dataset.sensors.items():
        # Halo overlap preserves local-window correctness near the partition
        # boundaries of each rank's time segment.
        segment = np.asarray(values[halo_start:halo_end], dtype=float)
        result = clean_sensor(segment, cleaning_cfg)
        trim_left = start - halo_start
        trim_right = trim_left + (end - start)
        local_payload[sensor] = {
            "segment": result.cleaned[trim_left:trim_right],
            "flags": result.flags[trim_left:trim_right].astype(np.uint8),
        }

    gathered = comm.gather((start, end, local_payload), root=0)
    summary = {"mode": "partitioned", "nproc": size, "rank": rank}
    if rank == 0:
        # Root rank reconstructs full-length arrays and writes the summary
        # statistics that the report can consume directly.
        out_dir = ensure_dir(output_dir)
        sensors_dir = ensure_dir(Path(out_dir) / "sensors")
        summary_rows = []
        for sensor in dataset.sensor_names():
            cleaned = np.empty(n, dtype=float)
            flags = np.empty(n, dtype=np.uint8)
            for start_i, end_i, payload in gathered:
                cleaned[start_i:end_i] = payload[sensor]["segment"]
                flags[start_i:end_i] = payload[sensor]["flags"]
            np.save(Path(sensors_dir) / f"{normalize_header(sensor)}_cleaned.npy", cleaned)
            np.save(Path(sensors_dir) / f"{normalize_header(sensor)}_flags.npy", flags)
            summary_rows.append(
                {
                    "sensor": sensor,
                    "n_points": int(n),
                    "n_flagged": int(flags.sum()),
                    "fraction_flagged": float(flags.mean()) if n else 0.0,
                    "mean_original": float(np.nanmean(dataset.sensors[sensor])),
                    "mean_cleaned": float(np.nanmean(cleaned)),
                    "std_original": float(np.nanstd(dataset.sensors[sensor])),
                    "std_cleaned": float(np.nanstd(cleaned)),
                    "min_cleaned": float(np.nanmin(cleaned)),
                    "max_cleaned": float(np.nanmax(cleaned)),
                }
            )
        write_csv_table(Path(out_dir) / "cleaning_summary.csv", summary_rows)
        summary["written_sensors"] = len(dataset.sensor_names())
    return summary
