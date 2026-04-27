'''
File description:
Plot-generation helpers for benchmark runtime and speedup figures.

The benchmark workflow writes both machine-readable JSON and human-readable
plots so results can be inspected quickly and embedded in the paper draft.
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


def plot_rate_of_change(
    time: np.ndarray,
    values: np.ndarray,
    out_path: str | Path,
    title: str | None = None,
    steady_segments: list[dict] | None = None,
) -> str | None:
    """Plot cleaned signal and d(signal)/dt against time."""
    time = np.asarray(time, dtype=float)
    values = np.asarray(values, dtype=float)
    order = np.argsort(time)
    time = time[order]
    values = values[order]
    mask = ~(np.isnan(time) | np.isnan(values))
    time = time[mask]
    values = values[mask]
    unique_time, unique_idx = np.unique(time, return_index=True)
    time = unique_time
    values = values[unique_idx]
    if time.size < 2:
        return None

    rate = np.gradient(values, time)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(8.5, 5.5), sharex=True)
    axes[0].plot(time, values, color="tab:blue", linewidth=2, label="cleaned signal")
    axes[1].plot(time, rate, color="tab:red", linewidth=2, label="rate of change")
    axes[1].axhline(0.0, color="black", linewidth=1, alpha=0.35)

    for segment in steady_segments or []:
        start = segment.get("start_time")
        end = segment.get("end_time")
        if start is None or end is None:
            continue
        axes[0].axvspan(start, end, color="tab:green", alpha=0.15)
        axes[1].axvspan(start, end, color="tab:green", alpha=0.15)

    axes[0].set_ylabel("Signal")
    axes[1].set_ylabel("d(signal)/dt")
    axes[1].set_xlabel("Time (s)")
    axes[0].grid(True, alpha=0.25)
    axes[1].grid(True, alpha=0.25)
    axes[0].legend(frameon=False, loc="upper left")
    axes[1].legend(frameon=False, loc="upper left")
    if title:
        fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return str(out_path)
