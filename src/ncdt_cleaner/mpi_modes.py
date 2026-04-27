'''
File description:
MPI execution strategies for replicated-data and partitioned-time cleaning.

This module contains the two parallel execution modes discussed in the report:
one simple replicated baseline and one partitioned approach with halo overlap.
'''

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import numpy as np
from ._mpi import MPI, ensure_mpi_initialized

from .behavior import analyze_signal_behavior, summarize_group_behaviors, write_behavior_outputs
from .cache import load_sensor_cache
from .characterize import characterize_signal
from .cleaning import clean_sensor
from .plotting import plot_rate_of_change
from .stats import write_csv_table
from .utils import ensure_dir, normalize_header

LOGGER = logging.getLogger(__name__)


def _rank_chunk(items: list[str], rank: int, size: int) -> list[str]:
    """Assign items to ranks using a simple round-robin pattern."""
    return [item for i, item in enumerate(items) if i % size == rank]


def run_replicated_mode(
    cache_dir: str | Path,
    output_dir: str | Path,
    cleaning_cfg: dict,
    char_cfg: dict,
    behavior_cfg: dict | None = None,
    do_characterize: bool = False,
) -> dict:
    """Run the replicated-data MPI strategy described in the paper."""
    ensure_mpi_initialized()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    t_load0 = time.perf_counter()
    dataset = load_sensor_cache(cache_dir, mmap_mode="r")
    load_elapsed = time.perf_counter() - t_load0
    sensor_names = dataset.sensor_names()
    local_sensors = _rank_chunk(sensor_names, rank, size)

    t_compute0 = time.perf_counter()
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
            steady_summary, steady_detail = analyze_signal_behavior(dataset.time, result.cleaned, behavior_cfg)
            payload["steady_state"] = steady_summary
            payload["steady_state_detail"] = steady_detail
        local_payload[sensor] = payload
    compute_elapsed = time.perf_counter() - t_compute0

    local_metrics = {
        "rank": rank,
        "assigned_sensors": len(local_sensors),
        "load_elapsed_sec": float(load_elapsed),
        "compute_elapsed_sec": float(compute_elapsed),
    }
    t_gather0 = time.perf_counter()
    gathered = comm.gather(local_payload, root=0)
    metrics_gathered = comm.gather(local_metrics, root=0)
    gather_elapsed = time.perf_counter() - t_gather0
    summary = {"mode": "replicated", "nproc": size, "rank": rank}
    if rank == 0:
        # Rank 0 merges payloads and performs all file writes for the mode.
        t_write0 = time.perf_counter()
        merged = {}
        for part in gathered:
            merged.update(part)
        out_dir = ensure_dir(output_dir)
        sensors_dir = ensure_dir(Path(out_dir) / "sensors")
        summary_rows = []
        characterization_rows = []
        behavior_summaries: dict[str, dict] = {}
        behavior_details: dict[str, dict] = {}
        for sensor, payload in merged.items():
            sensor_key = normalize_header(sensor)
            np.save(Path(sensors_dir) / f"{sensor_key}_cleaned.npy", payload["cleaned"])
            np.save(Path(sensors_dir) / f"{sensor_key}_flags.npy", payload["flags"])
            if "characterization" in payload:
                with open(Path(sensors_dir) / f"{sensor_key}_characterization.json", "w", encoding="utf-8") as f:
                    json.dump(payload["characterization"], f)
            if "steady_state" in payload:
                characterization_rows.append(
                    {
                        "sensor": sensor,
                        "method": payload["characterization"]["method"] if "characterization" in payload else None,
                        "n_dense": len(payload["characterization"]["dense_time"]) if "characterization" in payload else None,
                        "rate_of_change_plot": plot_rate_of_change(
                            dataset.time,
                            payload["cleaned"],
                            Path(sensors_dir) / f"{sensor_key}_rate_of_change.png",
                            title=f"{sensor}: cleaned signal and rate of change",
                            steady_segments=payload["steady_state"].get("steady_segments"),
                        ) if "steady_state" in payload else None,
                        "steady_state_summary": payload["steady_state"]["summary_text"],
                    }
                )
                behavior_summaries[sensor] = payload["steady_state"]
                behavior_details[sensor] = payload["steady_state_detail"]
            summary_rows.append({"sensor": sensor, **payload["stats"]})
        write_csv_table(Path(out_dir) / "cleaning_summary.csv", summary_rows)
        if characterization_rows:
            write_csv_table(Path(out_dir) / "characterization_summary.csv", characterization_rows)
        group_summaries = summarize_group_behaviors(behavior_details, behavior_cfg) if behavior_details else None
        behavior_artifacts = (
            write_behavior_outputs(out_dir, behavior_summaries, group_summaries=group_summaries)
            if behavior_summaries
            else None
        )
        write_elapsed = time.perf_counter() - t_write0
        assigned_counts = [int(item["assigned_sensors"]) for item in metrics_gathered]
        compute_times = [float(item["compute_elapsed_sec"]) for item in metrics_gathered]
        load_times = [float(item["load_elapsed_sec"]) for item in metrics_gathered]
        summary["written_sensors"] = len(summary_rows)
        summary["timing_breakdown"] = {
            "max_load_elapsed_sec": max(load_times) if load_times else 0.0,
            "max_compute_elapsed_sec": max(compute_times) if compute_times else 0.0,
            "gather_elapsed_sec": float(gather_elapsed),
            "root_write_elapsed_sec": float(write_elapsed),
        }
        summary["parallel_metrics"] = {
            "work_unit": "sensor",
            "total_sensors": len(sensor_names),
            "min_assigned_sensors": min(assigned_counts) if assigned_counts else 0,
            "max_assigned_sensors": max(assigned_counts) if assigned_counts else 0,
            "mean_assigned_sensors": float(sum(assigned_counts) / len(assigned_counts)) if assigned_counts else 0.0,
            "max_compute_over_min_compute": (
                max(compute_times) / min(value for value in compute_times if value > 0.0)
                if any(value > 0.0 for value in compute_times)
                else None
            ),
        }
        if behavior_artifacts:
            summary["behavior_artifacts"] = behavior_artifacts
    return summary


def run_partitioned_mode(
    cache_dir: str | Path,
    output_dir: str | Path,
    cleaning_cfg: dict,
    char_cfg: dict,
    behavior_cfg: dict | None = None,
    do_characterize: bool = False,
) -> dict:
    """Run the partitioned time-domain MPI strategy with halo overlap."""
    ensure_mpi_initialized()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    t_load0 = time.perf_counter()
    dataset = load_sensor_cache(cache_dir, mmap_mode="r")
    load_elapsed = time.perf_counter() - t_load0
    n = dataset.n_rows()
    window = int(cleaning_cfg["window_radius"])

    start = (rank * n) // size
    end = ((rank + 1) * n) // size
    halo_start = max(0, start - window)
    halo_end = min(n, end + window)

    t_compute0 = time.perf_counter()
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
    compute_elapsed = time.perf_counter() - t_compute0

    local_metrics = {
        "rank": rank,
        "owned_rows": int(end - start),
        "halo_rows": int((halo_end - halo_start) - (end - start)),
        "load_elapsed_sec": float(load_elapsed),
        "compute_elapsed_sec": float(compute_elapsed),
    }
    t_gather0 = time.perf_counter()
    gathered = comm.gather((start, end, local_payload), root=0)
    metrics_gathered = comm.gather(local_metrics, root=0)
    gather_elapsed = time.perf_counter() - t_gather0
    summary = {"mode": "partitioned", "nproc": size, "rank": rank}
    if rank == 0:
        # Root rank reconstructs full-length arrays and writes the summary
        # statistics that the report can consume directly.
        t_write0 = time.perf_counter()
        out_dir = ensure_dir(output_dir)
        sensors_dir = ensure_dir(Path(out_dir) / "sensors")
        summary_rows = []
        characterization_rows = []
        behavior_summaries: dict[str, dict] = {}
        behavior_details: dict[str, dict] = {}
        for sensor in dataset.sensor_names():
            cleaned = np.empty(n, dtype=float)
            flags = np.empty(n, dtype=np.uint8)
            for start_i, end_i, payload in gathered:
                cleaned[start_i:end_i] = payload[sensor]["segment"]
                flags[start_i:end_i] = payload[sensor]["flags"]
            sensor_key = normalize_header(sensor)
            np.save(Path(sensors_dir) / f"{sensor_key}_cleaned.npy", cleaned)
            np.save(Path(sensors_dir) / f"{sensor_key}_flags.npy", flags)
            if do_characterize:
                characterization = characterize_signal(dataset.time, cleaned, **char_cfg)
                behavior, behavior_detail = analyze_signal_behavior(dataset.time, cleaned, behavior_cfg)
                with open(Path(sensors_dir) / f"{sensor_key}_characterization.json", "w", encoding="utf-8") as f:
                    json.dump(characterization, f)
                characterization_rows.append(
                    {
                        "sensor": sensor,
                        "method": characterization["method"],
                        "n_dense": len(characterization["dense_time"]),
                        "rate_of_change_plot": plot_rate_of_change(
                            dataset.time,
                            cleaned,
                            Path(sensors_dir) / f"{sensor_key}_rate_of_change.png",
                            title=f"{sensor}: cleaned signal and rate of change",
                            steady_segments=behavior.get("steady_segments"),
                        ),
                        "steady_state_summary": behavior["summary_text"],
                    }
                )
                behavior_summaries[sensor] = behavior
                behavior_details[sensor] = behavior_detail
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
        if characterization_rows:
            write_csv_table(Path(out_dir) / "characterization_summary.csv", characterization_rows)
        group_summaries = summarize_group_behaviors(behavior_details, behavior_cfg) if behavior_details else None
        behavior_artifacts = (
            write_behavior_outputs(out_dir, behavior_summaries, group_summaries=group_summaries)
            if behavior_summaries
            else None
        )
        write_elapsed = time.perf_counter() - t_write0
        owned_rows = [int(item["owned_rows"]) for item in metrics_gathered]
        halo_rows = [int(item["halo_rows"]) for item in metrics_gathered]
        compute_times = [float(item["compute_elapsed_sec"]) for item in metrics_gathered]
        load_times = [float(item["load_elapsed_sec"]) for item in metrics_gathered]
        summary["written_sensors"] = len(dataset.sensor_names())
        summary["timing_breakdown"] = {
            "max_load_elapsed_sec": max(load_times) if load_times else 0.0,
            "max_compute_elapsed_sec": max(compute_times) if compute_times else 0.0,
            "gather_elapsed_sec": float(gather_elapsed),
            "root_write_elapsed_sec": float(write_elapsed),
        }
        summary["parallel_metrics"] = {
            "work_unit": "time_segment",
            "global_rows": int(n),
            "window_radius": int(window),
            "min_owned_rows": min(owned_rows) if owned_rows else 0,
            "max_owned_rows": max(owned_rows) if owned_rows else 0,
            "mean_owned_rows": float(sum(owned_rows) / len(owned_rows)) if owned_rows else 0.0,
            "mean_halo_rows": float(sum(halo_rows) / len(halo_rows)) if halo_rows else 0.0,
            "max_compute_over_min_compute": (
                max(compute_times) / min(value for value in compute_times if value > 0.0)
                if any(value > 0.0 for value in compute_times)
                else None
            ),
        }
        if behavior_artifacts:
            summary["behavior_artifacts"] = behavior_artifacts
    return summary
