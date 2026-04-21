'''
File description:
Standalone benchmark runner script built on top of the shared benchmark helpers.

This script remains useful for users who want only timing data from an existing
cache without invoking the broader workflow command.
'''

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ncdt_cleaner.benchmarks import run_scaling_study, write_benchmark_results
from ncdt_cleaner.cache import ensure_cache_for_input
from ncdt_cleaner.config import load_config, save_json
from ncdt_cleaner.session import create_session
from ncdt_cleaner.utils import ensure_dir


def main() -> int:
    """Parse arguments and run the standalone scaling-study script."""
    parser = argparse.ArgumentParser(description="Run serial and MPI scaling studies")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--cache-dir")
    source.add_argument("--input-file")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--sheet-name", default=None)
    parser.add_argument("--repeat", type=int, default=None)
    parser.add_argument("--process-counts", nargs="+", type=int, default=None)
    parser.add_argument("--modes", nargs="+", choices=["serial", "replicated", "partitioned"], default=None)
    parser.add_argument("--mpi-launcher", default="mpirun")
    parser.add_argument("--characterize", action="store_true")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    repeat = args.repeat or int(cfg["benchmark"]["repeat"])
    process_counts = [int(value) for value in (args.process_counts or cfg["benchmark"]["process_counts"])]
    benchmark_report = None

    if args.input_file is not None:
        session = create_session(cfg["analysis_dir"], "benchmark-auto", sys.argv)
        cache_resolution = ensure_cache_for_input(
            input_path=args.input_file,
            config_path=args.config,
            config=cfg,
            analysis_dir=cfg["analysis_dir"],
            sheet_name=args.sheet_name,
            target_cache_dir=session["paths"]["cache_dir"],
            target_session_dir=session["paths"]["session_dir"],
        )
        cache_dir = cache_resolution["cache_dir"]
        out_dir = ensure_dir(Path(args.out_dir) if args.out_dir is not None else Path(session["paths"]["outputs_dir"]) / "benchmark_auto")
        benchmark_report = Path(session["paths"]["outputs_dir"]) / "benchmark_auto_report.json"
    else:
        cache_resolution = {
            "status": "provided",
            "cache_dir": str(Path(args.cache_dir).resolve()),
            "cache_metadata_path": None,
            "matched_session_dir": None,
        }
        cache_dir = cache_resolution["cache_dir"]
        out_dir = ensure_dir(Path(args.out_dir) if args.out_dir is not None else Path("analysis") / "benchmark_runs")

    rows = run_scaling_study(
        cache_dir=cache_dir,
        config_path=args.config,
        repeat=repeat,
        process_counts=process_counts,
        modes=args.modes,
        characterize=args.characterize,
        mpi_launcher=args.mpi_launcher,
    )
    artifacts = write_benchmark_results(out_dir, rows)
    report = {
        "out_dir": str(out_dir),
        "cache_resolution": cache_resolution,
        "results": rows,
        "artifacts": artifacts,
    }
    if benchmark_report is not None:
        save_json(benchmark_report, report)
        report["benchmark_auto_report"] = str(benchmark_report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
