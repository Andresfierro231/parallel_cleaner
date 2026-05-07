"""Generate MPI diagnostics figures from strong- and weak-scaling analyses."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
FIGURE_DIR = ROOT / "reports" / "figures"
STRONG_DIR = ROOT / "analysis" / "parallel_diagnostics" / "strong_scaling_1m_detailed"
WEAK_DIR = ROOT / "analysis" / "weak_scaling" / "weak_scaling_parallel_report"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def make_timing_breakdown_figure() -> Path:
    rows = load_json(STRONG_DIR / "timing_breakdown_summary.json")["timing_breakdown_summary"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    for ax, mode in zip(axes, ("replicated", "partitioned")):
        mode_rows = sorted((row for row in rows if row["mode"] == mode), key=lambda row: row["nproc"])
        x = [row["nproc"] for row in mode_rows]
        ax.plot(x, [row.get("mean_max_compute_elapsed_sec") for row in mode_rows], marker="o", linewidth=2, label="compute")
        ax.plot(x, [row.get("mean_gather_elapsed_sec") for row in mode_rows], marker="o", linewidth=2, label="gather")
        ax.plot(x, [row.get("mean_root_write_elapsed_sec") for row in mode_rows], marker="o", linewidth=2, label="root write")
        ax.plot(x, [row.get("mean_max_load_elapsed_sec") for row in mode_rows], marker="o", linewidth=2, label="load")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("MPI ranks")
        ax.set_title(mode.capitalize())
        ax.grid(True, which="both", alpha=0.25)
    axes[0].set_ylabel("Stage time (s)")
    axes[1].legend(frameon=False, fontsize=9)
    fig.suptitle("Strong-Scaling Timing Breakdown (1M rows)", fontsize=13)
    fig.tight_layout()
    path = FIGURE_DIR / "mpi_timing_breakdown_strong_1m.png"
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return path


def make_weak_scaling_figures() -> tuple[Path, Path]:
    rows = load_json(WEAK_DIR / "weak_scaling_summary.json")["summary"]
    generated = []
    for metric, ylabel, filename in (
        ("elapsed_sec", "Runtime (s)", "weak_scaling_runtime_report.png"),
        ("weak_scaling_efficiency", "Weak-scaling efficiency", "weak_scaling_efficiency_report.png"),
    ):
        fig = plt.figure(figsize=(6.5, 4.5))
        ax = fig.add_subplot(111)
        for mode in sorted({row["mode"] for row in rows}):
            mode_rows = sorted((row for row in rows if row["mode"] == mode), key=lambda row: row["nproc"])
            ax.plot([row["nproc"] for row in mode_rows], [row[metric] for row in mode_rows], marker="o", linewidth=2, label=mode)
        ax.set_xscale("log", base=2)
        ax.set_xlabel("MPI ranks")
        ax.set_ylabel(ylabel)
        ax.grid(True, which="both", alpha=0.25)
        ax.legend(frameon=False)
        fig.tight_layout()
        path = FIGURE_DIR / filename
        fig.savefig(path, dpi=220, bbox_inches="tight")
        plt.close(fig)
        generated.append(path)
    return generated[0], generated[1]


def main() -> int:
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    outputs = {
        "timing_breakdown": str(make_timing_breakdown_figure()),
    }
    weak_runtime, weak_efficiency = make_weak_scaling_figures()
    outputs["weak_runtime"] = str(weak_runtime)
    outputs["weak_efficiency"] = str(weak_efficiency)
    print(json.dumps(outputs, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
