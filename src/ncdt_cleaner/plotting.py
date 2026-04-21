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

        generated[mode] = {
            "speedup_plot": str(speedup_path),
            "runtime_plot": str(runtime_path),
        }
    return generated
