"""Run a weak-scaling study built on synthetic benchmark datasets."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ncdt_cleaner.benchmarks import benchmark_subprocess, clean_cli_command, mpi_wrapped_command
from ncdt_cleaner.cache import ensure_cache_for_input
from ncdt_cleaner.config import load_config, save_json
from ncdt_cleaner.stats import write_csv_table
from ncdt_cleaner.synthetic import create_synthetic_campaign
from ncdt_cleaner.utils import ensure_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a weak-scaling MPI study")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--campaign-name", default="weak_scaling_default")
    parser.add_argument("--campaign-root", default="analysis/synthetic_campaigns")
    parser.add_argument("--out-dir", default="analysis/weak_scaling/weak_scaling_default")
    parser.add_argument("--process-counts", nargs="+", type=int, default=[1, 2, 4, 8, 16, 24])
    parser.add_argument("--rows-per-rank", type=int, default=125000)
    parser.add_argument("--num-sensors", type=int, default=8)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--modes", nargs="+", choices=["serial", "replicated", "partitioned"], default=["replicated", "partitioned"])
    parser.add_argument("--mpi-launcher", default="mpirun")
    parser.add_argument("--noise-sigma", type=float, default=0.35)
    parser.add_argument("--spike-fraction", type=float, default=0.002)
    parser.add_argument("--flat-fraction", type=float, default=0.0005)
    parser.add_argument("--dropout-fraction", type=float, default=0.0002)
    parser.add_argument("--seed-base", type=int, default=400)
    return parser


def dataset_path_for_rows(campaign_dir: Path, rows: int, num_sensors: int) -> Path:
    matches = sorted((campaign_dir / "datasets").glob(f"*_rows_{rows}_sensors_{num_sensors}_*.csv"))
    if not matches:
        raise FileNotFoundError(f"No dataset found for rows={rows} sensors={num_sensors} under {campaign_dir}")
    return matches[0]


def summarize_weak_rows(rows: list[dict]) -> list[dict]:
    summary = []
    by_mode: dict[str, list[dict]] = {}
    for row in rows:
        by_mode.setdefault(row["mode"], []).append(row)
    for mode, mode_rows in by_mode.items():
        ordered = sorted(mode_rows, key=lambda row: int(row["nproc"]))
        base = ordered[0]
        base_elapsed = float(base["elapsed_sec"])
        for row in ordered:
            elapsed = float(row["elapsed_sec"])
            summary.append(
                {
                    "mode": mode,
                    "nproc": int(row["nproc"]),
                    "rows_per_rank": int(row["rows_per_rank"]),
                    "total_rows": int(row["total_rows"]),
                    "elapsed_sec": elapsed,
                    "weak_scaling_efficiency": base_elapsed / elapsed if elapsed else None,
                    "normalized_elapsed": elapsed / base_elapsed if base_elapsed else None,
                }
            )
    return summary


def write_weak_plots(summary_rows: list[dict], out_dir: Path) -> dict[str, str]:
    out_dir = ensure_dir(out_dir / "plots")
    generated = {}
    for metric, ylabel, filename in (
        ("elapsed_sec", "Runtime (s)", "weak_scaling_runtime.png"),
        ("weak_scaling_efficiency", "Weak-scaling efficiency", "weak_scaling_efficiency.png"),
    ):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        for mode in sorted({row["mode"] for row in summary_rows}):
            rows = sorted((row for row in summary_rows if row["mode"] == mode), key=lambda row: row["nproc"])
            ax.plot([row["nproc"] for row in rows], [row[metric] for row in rows], marker="o", linewidth=2, label=mode)
        ax.set_xscale("log", base=2)
        ax.set_xlabel("MPI ranks")
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel)
        ax.grid(True, which="both", alpha=0.25)
        ax.legend(frameon=False)
        path = out_dir / filename
        fig.savefig(path, dpi=220, bbox_inches="tight")
        plt.close(fig)
        generated[metric] = str(path)
    return generated


def main() -> int:
    args = build_parser().parse_args()
    cfg = load_config(args.config)

    campaign_dir = ensure_dir(Path(args.campaign_root) / args.campaign_name)
    out_dir = ensure_dir(Path(args.out_dir))
    process_counts = [int(value) for value in args.process_counts]
    row_counts = [int(args.rows_per_rank) * nproc for nproc in process_counts]

    campaign = create_synthetic_campaign(
        out_dir=campaign_dir,
        campaign_name=args.campaign_name,
        row_counts=row_counts,
        num_sensors=int(args.num_sensors),
        noise_sigma=float(args.noise_sigma),
        spike_fraction=float(args.spike_fraction),
        seed_base=int(args.seed_base),
        flat_fraction=float(args.flat_fraction),
        dropout_fraction=float(args.dropout_fraction),
        formats=("csv",),
    )

    rows: list[dict] = []
    logs_dir = ensure_dir(out_dir / "logs")
    for nproc, total_rows in zip(process_counts, row_counts):
        input_path = dataset_path_for_rows(campaign_dir, total_rows, int(args.num_sensors))
        cache_session_dir = ensure_dir(out_dir / "cache_sessions" / f"rows_{total_rows}")
        cache_dir = ensure_dir(cache_session_dir / "cache")
        cache_resolution = ensure_cache_for_input(
            input_path=input_path,
            config_path=args.config,
            config=cfg,
            analysis_dir=cfg["analysis_dir"],
            target_cache_dir=cache_dir,
            target_session_dir=cache_session_dir,
        )
        resolved_cache_dir = cache_resolution["cache_dir"]
        for mode in args.modes:
            command = clean_cli_command(
                config_path=args.config,
                cache_dir=resolved_cache_dir,
                mode=mode,
                characterize=False,
                python_executable=sys.executable,
            )
            actual_nproc = nproc if mode != "serial" else 1
            if mode != "serial":
                command = mpi_wrapped_command(command, nproc=nproc, mpi_launcher=args.mpi_launcher)
            result = benchmark_subprocess(
                command,
                repeat=int(args.repeat),
                log_dir=logs_dir,
                log_prefix=f"{mode}_rows{total_rows}_n{actual_nproc}",
            )
            rows.append(
                {
                    "mode": mode,
                    "nproc": int(actual_nproc),
                    "rows_per_rank": int(args.rows_per_rank),
                    "total_rows": int(total_rows),
                    "elapsed_sec": float(result["elapsed_sec"]),
                    "all_times": result["all_times"],
                    "command": result["command"],
                    "child_payloads": result["child_payloads"],
                    "stdout_logs": result["stdout_logs"],
                    "stderr_logs": result["stderr_logs"],
                }
            )

    summary_rows = summarize_weak_rows(rows)
    save_json(out_dir / "weak_scaling_results.json", {"results": rows})
    save_json(out_dir / "weak_scaling_summary.json", {"summary": summary_rows})
    write_csv_table(out_dir / "weak_scaling_summary.csv", summary_rows)
    plots = write_weak_plots(summary_rows, out_dir)
    manifest = {
        "campaign": campaign,
        "config_path": args.config,
        "process_counts": process_counts,
        "rows_per_rank": int(args.rows_per_rank),
        "num_sensors": int(args.num_sensors),
        "repeat": int(args.repeat),
        "modes": list(args.modes),
        "mpi_launcher": args.mpi_launcher,
        "plots": plots,
    }
    save_json(out_dir / "weak_scaling_manifest.json", manifest)
    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "results_json": str(out_dir / "weak_scaling_results.json"),
                "summary_json": str(out_dir / "weak_scaling_summary.json"),
                "summary_csv": str(out_dir / "weak_scaling_summary.csv"),
                "manifest_json": str(out_dir / "weak_scaling_manifest.json"),
                "plots": plots,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
