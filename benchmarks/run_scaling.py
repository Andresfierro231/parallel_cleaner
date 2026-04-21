'''
File description:
Standalone benchmark runner script built on top of the shared benchmark helpers.

This script remains useful for users who want only timing data from an existing
cache without invoking the broader workflow command.
'''

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ncdt_cleaner.benchmarks import run_scaling_study, write_benchmark_results
from ncdt_cleaner.config import load_config
from ncdt_cleaner.utils import ensure_dir


def main() -> int:
    """Parse arguments and run the standalone scaling-study script."""
    parser = argparse.ArgumentParser(description="Run serial and MPI scaling studies")
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--repeat", type=int, default=None)
    parser.add_argument("--process-counts", nargs="+", type=int, default=None)
    parser.add_argument("--modes", nargs="+", choices=["serial", "replicated", "partitioned"], default=None)
    parser.add_argument("--mpi-launcher", default="mpirun")
    parser.add_argument("--characterize", action="store_true")
    parser.add_argument("--out-dir", default="analysis/benchmark_runs")
    args = parser.parse_args()

    cfg = load_config(args.config)
    repeat = args.repeat or int(cfg["benchmark"]["repeat"])
    process_counts = [int(value) for value in (args.process_counts or cfg["benchmark"]["process_counts"])]

    rows = run_scaling_study(
        cache_dir=args.cache_dir,
        config_path=args.config,
        repeat=repeat,
        process_counts=process_counts,
        modes=args.modes,
        characterize=args.characterize,
        mpi_launcher=args.mpi_launcher,
    )
    out_dir = ensure_dir(Path(args.out_dir))
    artifacts = write_benchmark_results(out_dir, rows)
    print(json.dumps({"out_dir": str(out_dir), "results": rows, "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
