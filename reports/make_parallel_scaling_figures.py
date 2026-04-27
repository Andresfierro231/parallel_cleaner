"""Generate combined scaling figures for the paper from corrected summaries."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
SUMMARY_ROOT = ROOT / "analysis" / "full_matrix" / "paper_scaling_100k_1m"
FIGURE_DIR = ROOT / "reports" / "figures"


DATASET_LABELS = {
    "paper_scaling_100k_1m_rows_100000_sensors_8_seed_100": "100k rows",
    "paper_scaling_100k_1m_rows_200000_sensors_8_seed_101": "200k rows",
    "paper_scaling_100k_1m_rows_500000_sensors_8_seed_102": "500k rows",
    "paper_scaling_100k_1m_rows_1000000_sensors_8_seed_103": "1M rows",
}

MODE_TITLES = {
    "replicated": "Replicated MPI",
    "partitioned": "Partitioned MPI",
}


def load_summary(path: Path) -> list[dict]:
    return json.loads(path.read_text())["summary"]


def dataset_series(summary_rows: list[dict], mode: str) -> tuple[list[int], list[float], list[float], list[float]]:
    serial = next(row for row in summary_rows if row["mode"] == "serial" and int(row["nproc"]) == 1)
    rows = [row for row in summary_rows if row["mode"] == mode]
    return (
        [int(row["nproc"]) for row in rows],
        [float(row["elapsed_sec"]) for row in rows],
        [float(row["speedup_vs_serial"]) for row in rows],
        [float(row["parallel_efficiency"]) for row in rows],
    )


def collect_summaries() -> list[tuple[str, list[dict]]]:
    datasets = []
    for summary_path in sorted(SUMMARY_ROOT.glob("*/benchmark_summary.json")):
        dataset_name = summary_path.parent.name
        if dataset_name not in DATASET_LABELS:
            continue
        datasets.append((dataset_name, load_summary(summary_path)))
    if not datasets:
        raise FileNotFoundError(f"No benchmark summaries found under {SUMMARY_ROOT}")
    return datasets


def make_runtime_figure(datasets: list[tuple[str, list[dict]]]) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    for ax, mode in zip(axes, ("replicated", "partitioned")):
        for dataset_name, rows in datasets:
            procs, elapsed, _, _ = dataset_series(rows, mode)
            ax.plot(procs, elapsed, marker="o", linewidth=2, label=DATASET_LABELS[dataset_name])
        ax.set_title(MODE_TITLES[mode])
        ax.set_xlabel("MPI ranks")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.grid(True, which="both", alpha=0.25)
    axes[0].set_ylabel("Runtime (s)")
    axes[1].legend(frameon=False, fontsize=9)
    fig.suptitle("Corrected Runtime Scaling Across Dataset Sizes", fontsize=13)
    fig.tight_layout()
    output_path = FIGURE_DIR / "combined_runtime_scaling.png"
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return output_path


def make_speedup_figure(datasets: list[tuple[str, list[dict]]]) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    for ax, mode in zip(axes, ("replicated", "partitioned")):
        all_proc_counts: set[int] = set()
        for dataset_name, rows in datasets:
            procs, _, speedup, _ = dataset_series(rows, mode)
            all_proc_counts.update(procs)
            ax.plot(procs, speedup, marker="o", linewidth=2, label=DATASET_LABELS[dataset_name])
        ideal = sorted(all_proc_counts)
        ax.plot(ideal, ideal, linestyle="--", color="black", linewidth=1, label="Ideal" if mode == "partitioned" else None)
        ax.set_title(MODE_TITLES[mode])
        ax.set_xlabel("MPI ranks")
        ax.set_xscale("log", base=2)
        ax.grid(True, which="both", alpha=0.25)
    axes[0].set_ylabel("Speedup vs serial")
    axes[1].legend(frameon=False, fontsize=9)
    fig.suptitle("Corrected Speedup Scaling Across Dataset Sizes", fontsize=13)
    fig.tight_layout()
    output_path = FIGURE_DIR / "combined_speedup_scaling.png"
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return output_path


def make_efficiency_figure(datasets: list[tuple[str, list[dict]]]) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    for ax, mode in zip(axes, ("replicated", "partitioned")):
        for dataset_name, rows in datasets:
            procs, _, _, efficiency = dataset_series(rows, mode)
            ax.plot(procs, efficiency, marker="o", linewidth=2, label=DATASET_LABELS[dataset_name])
        ax.set_title(MODE_TITLES[mode])
        ax.set_xlabel("MPI ranks")
        ax.set_xscale("log", base=2)
        ax.grid(True, which="both", alpha=0.25)
    axes[0].set_ylabel("Parallel efficiency")
    axes[1].legend(frameon=False, fontsize=9)
    fig.suptitle("Corrected Parallel Efficiency Across Dataset Sizes", fontsize=13)
    fig.tight_layout()
    output_path = FIGURE_DIR / "combined_efficiency_scaling.png"
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> int:
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    datasets = collect_summaries()
    outputs = {
        "runtime": str(make_runtime_figure(datasets)),
        "speedup": str(make_speedup_figure(datasets)),
        "efficiency": str(make_efficiency_figure(datasets)),
    }
    print(json.dumps(outputs, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
