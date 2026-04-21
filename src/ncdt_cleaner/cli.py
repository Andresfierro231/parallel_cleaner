from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from .cache import load_sensor_cache, write_sensor_cache
from .characterize import characterize_signal
from .cleaning import clean_sensor
from .config import load_config, save_json
from .inspectors import inspect_file
from .mpi_modes import run_partitioned_mode, run_replicated_mode
from .normalization import dataframe_to_sensor_dataset
from .readers import read_tabular_file
from .session import create_session
from .stats import dataset_summary, write_csv_table
from .synthetic import generate_synthetic_timeseries
from .utils import ensure_dir, normalize_header
from .workflow import run_workflow

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NCDT parallel cleaner CLI")
    parser.add_argument("--config", default="configs/default_config.json")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("inspect")
    p.add_argument("inputs", nargs="+")

    p = sub.add_parser("cache-build")
    p.add_argument("input")
    p.add_argument("--sheet-name", default=None)

    p = sub.add_parser("clean")
    p.add_argument("--cache-dir", required=True)
    p.add_argument("--mode", choices=["serial", "replicated", "partitioned"], default="serial")
    p.add_argument("--characterize", action="store_true")

    p = sub.add_parser("characterize")
    p.add_argument("--cache-dir", required=True)

    p = sub.add_parser("synth")
    p.add_argument("--out", required=True)
    p.add_argument("--n-rows", type=int, default=200000)
    p.add_argument("--n-sensors", type=int, default=8)

    p = sub.add_parser("workflow")
    p.add_argument("input", nargs="?")
    p.add_argument("--cache-dir", default=None)
    p.add_argument("--sheet-name", default=None)
    p.add_argument("--modes", nargs="+", choices=["serial", "replicated", "partitioned"], default=list(("serial", "replicated", "partitioned")))
    p.add_argument("--process-counts", nargs="+", type=int, default=None)
    p.add_argument("--repeat", type=int, default=None)
    p.add_argument("--analysis-nproc", type=int, default=None)
    p.add_argument("--mpi-launcher", default="mpirun")
    p.add_argument("--characterize", action="store_true")
    p.add_argument("--skip-inspect", action="store_true")
    p.add_argument("--skip-clean-runs", action="store_true")
    p.add_argument("--skip-benchmark", action="store_true")

    return parser


def cmd_inspect(args, cfg, session):
    reports = [inspect_file(path, cfg) for path in args.inputs]
    out_path = Path(session["paths"]["outputs_dir"]) / "inspection_report.json"
    save_json(out_path, {"files": reports})
    print(json.dumps({"inspection_report": str(out_path), "files": reports}, indent=2))


def cmd_cache_build(args, cfg, session):
    df = read_tabular_file(args.input, sheet_name=args.sheet_name)
    dataset, summary = dataframe_to_sensor_dataset(df, Path(args.input).stem, cfg)
    cache_dir = Path(session["paths"]["cache_dir"])
    write_sensor_cache(dataset, cache_dir, dtype=cfg["cache"]["dtype"])
    save_json(Path(session["paths"]["outputs_dir"]) / "normalized_summary.json", summary)
    print(json.dumps({"cache_dir": str(cache_dir), "summary": summary}, indent=2))


def cmd_clean(args, cfg, session):
    from ._mpi import MPI, MPI_AVAILABLE
    
    output_dir = ensure_dir(Path(session["paths"]["outputs_dir"]) / f"clean_{args.mode}")
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    t0 = time.perf_counter()

    if args.mode in {"replicated", "partitioned"} and not MPI_AVAILABLE:
        raise RuntimeError("mpi4py is not available in this environment; install mpi4py to use MPI modes")

    if args.mode == "serial":
        if rank != 0:
            return
        dataset = load_sensor_cache(args.cache_dir, mmap_mode="r")
        summary_rows = []
        sensors_dir = ensure_dir(output_dir / "sensors")
        for sensor, values in dataset.sensors.items():
            result = clean_sensor(values, cfg["cleaning"])
            import numpy as np
            np.save(sensors_dir / f"{normalize_header(sensor)}_cleaned.npy", result.cleaned)
            np.save(sensors_dir / f"{normalize_header(sensor)}_flags.npy", result.flags.astype('uint8'))
            if args.characterize:
                char = characterize_signal(dataset.time, result.cleaned, **cfg["characterization"])
                with open(sensors_dir / f"{normalize_header(sensor)}_characterization.json", "w", encoding="utf-8") as f:
                    json.dump(char, f)
            summary_rows.append({"sensor": sensor, **result.stats})
        write_csv_table(output_dir / "cleaning_summary.csv", summary_rows)
        elapsed = time.perf_counter() - t0
        save_json(output_dir / "run_summary.json", {"mode": "serial", "nproc": 1, "elapsed_sec": elapsed})
        print(
            json.dumps(
                {
                    "mode": "serial",
                    "elapsed_sec": elapsed,
                    "output_dir": str(output_dir),
                    "run_summary_json": str(output_dir / "run_summary.json"),
                    "cleaning_summary_csv": str(output_dir / "cleaning_summary.csv"),
                },
                indent=2,
            )
        )
        return

    if args.mode == "replicated":
        summary = run_replicated_mode(args.cache_dir, output_dir, cfg["cleaning"], cfg["characterization"], do_characterize=args.characterize)
    else:
        summary = run_partitioned_mode(args.cache_dir, output_dir, cfg["cleaning"])

    elapsed = time.perf_counter() - t0
    if rank == 0:
        summary["elapsed_sec"] = elapsed
        save_json(output_dir / "run_summary.json", summary)
        print(
            json.dumps(
                {
                    "output_dir": str(output_dir),
                    "run_summary_json": str(output_dir / "run_summary.json"),
                    "cleaning_summary_csv": str(output_dir / "cleaning_summary.csv"),
                    **summary,
                },
                indent=2,
            )
        )


def cmd_characterize(args, cfg, session):
    dataset = load_sensor_cache(args.cache_dir, mmap_mode="r")
    out_dir = ensure_dir(Path(session["paths"]["outputs_dir"]) / "characterization")
    rows = []
    for sensor, values in dataset.sensors.items():
        char = characterize_signal(dataset.time, values, **cfg["characterization"])
        with open(out_dir / f"{normalize_header(sensor)}_characterization.json", "w", encoding="utf-8") as f:
            json.dump(char, f)
        rows.append({"sensor": sensor, "method": char["method"], "n_dense": len(char["dense_time"])})
    write_csv_table(out_dir / "characterization_summary.csv", rows)
    print(json.dumps({"output_dir": str(out_dir), "characterized_sensors": len(rows)}, indent=2))


def cmd_synth(args, cfg, session):
    out = generate_synthetic_timeseries(args.out, n_rows=args.n_rows, n_sensors=args.n_sensors)
    print(json.dumps({"synthetic_csv": str(out)}, indent=2))


def cmd_workflow(args, cfg, session):
    report = run_workflow(args, cfg, session)
    print(json.dumps(report, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    session = create_session(cfg["analysis_dir"], args.command, sys.argv)
    if args.command == "inspect":
        cmd_inspect(args, cfg, session)
    elif args.command == "cache-build":
        cmd_cache_build(args, cfg, session)
    elif args.command == "clean":
        cmd_clean(args, cfg, session)
    elif args.command == "characterize":
        cmd_characterize(args, cfg, session)
    elif args.command == "synth":
        cmd_synth(args, cfg, session)
    elif args.command == "workflow":
        cmd_workflow(args, cfg, session)
    else:
        parser.error(f"Unknown command {args.command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
