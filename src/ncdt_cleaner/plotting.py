'''
File description:
Plot-generation helpers for benchmark figures and per-sensor signal overlays.

The benchmark workflow writes both machine-readable JSON and human-readable
plots so results can be inspected quickly and reused in technical writeups.
'''

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def plot_speedup(results: list[dict], out_dir: str | Path) -> dict:
    """Generate per-mode runtime and speedup plots from benchmark rows."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    by_mode: dict[str, list[dict]] = {}
    for row in results:
        by_mode.setdefault(row["mode"], []).append(row)

    generated = {}
    for mode, rows in by_mode.items():
        rows = sorted(rows, key=lambda r: r["nproc"])
        p = np.array([r["nproc"] for r in rows], dtype=float)
        t = np.array([r["elapsed_sec"] for r in rows], dtype=float)
        t1 = t[0] if t.size else 1.0
        speedup = t1 / t

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(p, speedup, marker="o")
        ax.plot(p, p, linestyle="--")
        ax.set_xlabel("Processes")
        ax.set_ylabel("Speedup")
        ax.set_title(f"Speedup vs processes ({mode})")
        speedup_path = out_dir / f"speedup_{mode}.png"
        fig.savefig(speedup_path, dpi=180, bbox_inches="tight")
        plt.close(fig)

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(p, t, marker="o")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("Processes (log2)")
        ax.set_ylabel("Runtime (s, log)")
        ax.set_title(f"Runtime scaling ({mode})")
        runtime_path = out_dir / f"runtime_{mode}.png"
        fig.savefig(runtime_path, dpi=180, bbox_inches="tight")
        plt.close(fig)

        fig = plt.figure()
        ax = fig.add_subplot(111)
        efficiency = speedup / np.maximum(p, 1.0)
        ax.plot(p, efficiency, marker="o")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Processes (log2)")
        ax.set_ylabel("Parallel efficiency")
        ax.set_title(f"Parallel efficiency ({mode})")
        efficiency_path = out_dir / f"efficiency_{mode}.png"
        fig.savefig(efficiency_path, dpi=180, bbox_inches="tight")
        plt.close(fig)

        generated[mode] = {
            "speedup_plot": str(speedup_path),
            "runtime_plot": str(runtime_path),
            "efficiency_plot": str(efficiency_path),
        }
    return generated


def _sorted_unique_series(time: np.ndarray, values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return one numeric time series with sorted, unique, non-missing samples."""
    time = np.asarray(time, dtype=float)
    values = np.asarray(values, dtype=float)
    order = np.argsort(time)
    time = time[order]
    values = values[order]
    mask = ~(np.isnan(time) | np.isnan(values))
    time = time[mask]
    values = values[mask]
    unique_time, unique_idx = np.unique(time, return_index=True)
    return unique_time, values[unique_idx]


def plot_signal_overlay(
    time: np.ndarray,
    raw_values: np.ndarray,
    cleaned_values: np.ndarray,
    dense_time: np.ndarray | list[float] | None,
    dense_values: np.ndarray | list[float] | None,
    out_path: str | Path,
    title: str | None = None,
    steady_segments: list[dict] | None = None,
) -> str | None:
    """Plot raw samples, cleaned signal, and dense spline/interpolation curve."""
    raw_time, raw_values = _sorted_unique_series(time, raw_values)
    clean_time, cleaned_values = _sorted_unique_series(time, cleaned_values)
    if clean_time.size < 2:
        return None

    dense_time_arr = np.asarray(dense_time if dense_time is not None else [], dtype=float)
    dense_values_arr = np.asarray(dense_values if dense_values is not None else [], dtype=float)
    if dense_time_arr.size and dense_values_arr.size:
        dense_time_arr, dense_values_arr = _sorted_unique_series(dense_time_arr, dense_values_arr)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(1, 1, figsize=(8.5, 4.8))
    if raw_time.size:
        ax.scatter(raw_time, raw_values, color="tab:gray", s=14, alpha=0.55, label="raw data")
    if not np.array_equal(raw_values, cleaned_values) or not np.array_equal(raw_time, clean_time):
        ax.plot(clean_time, cleaned_values, color="tab:blue", linewidth=1.8, label="cleaned signal")
    if dense_time_arr.size and dense_values_arr.size:
        ax.plot(dense_time_arr, dense_values_arr, color="tab:orange", linewidth=2.2, label="spline curve")

    for segment in steady_segments or []:
        start = segment.get("start_time")
        end = segment.get("end_time")
        if start is None or end is None:
            continue
        ax.axvspan(start, end, color="tab:green", alpha=0.12)

    ax.set_ylabel("Signal")
    ax.set_xlabel("Time (s)")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False, loc="best")
    if title:
        fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return str(out_path)
