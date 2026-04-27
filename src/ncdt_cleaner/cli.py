'''
File description:
Command-line interface for the NCDT cleaner project.

New users should start here if they want to understand the available commands,
how sessions are created, and how the `workflow` entrypoint ties together
inspection, cache creation, cleaning, and benchmarking.
'''

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from .behavior import analyze_signal_behavior, summarize_group_behaviors, write_behavior_outputs
from .cache import load_sensor_cache, write_cache_metadata, write_sensor_cache
from .characterize import characterize_signal
from .cleaning import clean_sensor
from .config import load_config, save_json
from .inspectors import inspect_file
from .mpi_modes import run_partitioned_mode, run_replicated_mode
from .normalization import dataframe_to_sensor_dataset
from .plotting import plot_rate_of_change
from .readers import read_tabular_file
from .session import create_session
from .stats import write_csv_table
from .synthetic import generate_synthetic_timeseries
from .utils import ensure_dir, normalize_header
from .workflow import run_workflow

LOGGER = logging.getLogger(__name__)


def _add_steady_state_override_arg(parser: argparse.ArgumentParser) -> None:
    """Add the user-facing steady-state convenience override to a parser."""
    parser.add_argument(
        "--steady-window-seconds",
        type=float,
        default=None,
        help=(
            "Override the steady-state window length in seconds. "
            "For convenience, this also sets the minimum required steady duration."
        ),
    )


def _apply_cli_overrides(cfg: dict, args) -> dict:
    """Apply small user-facing CLI overrides without rewriting the config file."""
    effective = json.loads(json.dumps(cfg))
    steady_window = getattr(args, "steady_window_seconds", None)
    if steady_window is not None:
        effective.setdefault("steady_state", {})
        effective["steady_state"]["steady_window_seconds"] = float(steady_window)
        effective["steady_state"]["min_steady_duration_seconds"] = float(steady_window)
    return effective


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser and all supported subcommands."""
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
    _add_steady_state_override_arg(p)

    p = sub.add_parser("characterize")
    p.add_argument("--cache-dir", required=True)
    _add_steady_state_override_arg(p)

    p = sub.add_parser("synth")
    p.add_argument("--out", required=True)
    p.add_argument("--n-rows", type=int, default=200000)
    p.add_argument("--n-sensors", type=int, default=8)
    p.add_argument("--noise-sigma", type=float, default=0.1)
    p.add_argument("--spike-fraction", type=float, default=0.002)
    p.add_argument("--seed", type=int, default=1234)
    p.add_argument("--time-mode", choices=["numeric", "datetime", "indexless"], default="numeric")
    p.add_argument("--header-style", choices=["standard", "irregular"], default="standard")
    p.add_argument("--include-junk-columns", action="store_true")
    p.add_argument("--flat-fraction", type=float, default=0.0)
    p.add_argument("--dropout-fraction", type=float, default=0.0)
    p.add_argument("--output-format", choices=["csv", "json", "ndjson", "xlsx"], default=None)

    p = sub.add_parser("workflow")
    p.add_argument("input", nargs="?")
    p.add_argument("--cache-dir", default=None)
    p.add_argument("--sheet-name", default=None)
    p.add_argument("--modes", nargs="+", choices=["serial", "replicated", "partitioned"], default=["serial", "replicated", "partitioned"])
    p.add_argument("--process-counts", nargs="+", type=int, default=None)
    p.add_argument("--repeat", type=int, default=None)
    p.add_argument("--analysis-nproc", type=int, default=None)
    p.add_argument("--mpi-launcher", default="mpirun")
    p.add_argument("--characterize", action="store_true")
    p.add_argument("--skip-inspect", action="store_true")
    p.add_argument("--skip-clean-runs", action="store_true")
    p.add_argument("--skip-benchmark", action="store_true")
    _add_steady_state_override_arg(p)

    return parser


def cmd_inspect(args, cfg, session):
    """Run lightweight inspection on one or more raw input files."""
    reports = [inspect_file(path, cfg) for path in args.inputs]
    out_path = Path(session["paths"]["outputs_dir"]) / "inspection_report.json"
    save_json(out_path, {"files": reports})
    print(json.dumps({"inspection_report": str(out_path), "files": reports}, indent=2))


def cmd_cache_build(args, cfg, session):
    """Normalize a raw input file and write its binary cache representation."""
    df = read_tabular_file(args.input, sheet_name=args.sheet_name)
    dataset, summary = dataframe_to_sensor_dataset(df, Path(args.input).stem, cfg)
    cache_dir = Path(session["paths"]["cache_dir"])
    write_sensor_cache(dataset, cache_dir, dtype=cfg["cache"]["dtype"])
    cache_metadata_path = write_cache_metadata(
        cache_dir,
        input_path=args.input,
        config_path=args.config,
        dataset_summary=summary,
        sheet_name=args.sheet_name,
        session_dir=session["paths"]["session_dir"],
    )
    save_json(Path(session["paths"]["outputs_dir"]) / "normalized_summary.json", summary)
    print(
        json.dumps(
            {
                "cache_dir": str(cache_dir),
                "cache_metadata_path": str(cache_metadata_path),
                "summary": summary,
            },
            indent=2,
        )
    )


def cmd_clean(args, cfg, session):
    """Execute one cleaning mode against an existing cache directory."""
    from ._mpi import MPI, MPI_AVAILABLE, ensure_mpi_initialized, finalize_mpi
    cfg = _apply_cli_overrides(cfg, args)
    output_dir = ensure_dir(Path(session["paths"]["outputs_dir"]) / f"clean_{args.mode}")
    rank = 0
    comm = None
    t0 = time.perf_counter()

    if args.mode in {"replicated", "partitioned"} and not MPI_AVAILABLE:
        raise RuntimeError("mpi4py is not available in this environment; install mpi4py to use MPI modes")

    if args.mode in {"replicated", "partitioned"}:
        # MPI is initialized only for MPI modes so plain `--help` and serial
        # commands still work on systems where mpi4py is installed.
        ensure_mpi_initialized()
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()

    if args.mode == "serial":
        # Serial mode is the simplest correctness baseline and does not rely on
        # any MPI communication.
        t_load0 = time.perf_counter()
        dataset = load_sensor_cache(args.cache_dir, mmap_mode="r")
        load_elapsed = time.perf_counter() - t_load0
        summary_rows = []
        characterization_rows = []
        behavior_summaries: dict[str, dict] = {}
        behavior_details: dict[str, dict] = {}
        sensors_dir = ensure_dir(output_dir / "sensors")
        compute_elapsed = 0.0
        characterize_elapsed = 0.0
        write_elapsed = 0.0
        for sensor, values in dataset.sensors.items():
            t_compute0 = time.perf_counter()
            result = clean_sensor(values, cfg["cleaning"])
            compute_elapsed += time.perf_counter() - t_compute0
            import numpy as np
            t_write0 = time.perf_counter()
            np.save(sensors_dir / f"{normalize_header(sensor)}_cleaned.npy", result.cleaned)
            np.save(sensors_dir / f"{normalize_header(sensor)}_flags.npy", result.flags.astype('uint8'))
            write_elapsed += time.perf_counter() - t_write0
            if args.characterize:
                t_char0 = time.perf_counter()
                char = characterize_signal(dataset.time, result.cleaned, **cfg["characterization"])
                behavior, behavior_detail = analyze_signal_behavior(dataset.time, result.cleaned, cfg.get("steady_state"))
                characterize_elapsed += time.perf_counter() - t_char0
                t_char_write0 = time.perf_counter()
                with open(sensors_dir / f"{normalize_header(sensor)}_characterization.json", "w", encoding="utf-8") as f:
                    json.dump(char, f)
                write_elapsed += time.perf_counter() - t_char_write0
                characterization_rows.append(
                    {
                        "sensor": sensor,
                        "method": char["method"],
                        "n_dense": len(char["dense_time"]),
                        "rate_of_change_plot": plot_rate_of_change(
                            dataset.time,
                            result.cleaned,
                            sensors_dir / f"{normalize_header(sensor)}_rate_of_change.png",
                            title=f"{sensor}: cleaned signal and rate of change",
                            steady_segments=behavior.get("steady_segments"),
                        ),
                        "steady_state_summary": behavior["summary_text"],
                    }
                )
                behavior_summaries[sensor] = behavior
                behavior_details[sensor] = behavior_detail
            summary_rows.append({"sensor": sensor, **result.stats})
        t_csv0 = time.perf_counter()
        write_csv_table(output_dir / "cleaning_summary.csv", summary_rows)
        if characterization_rows:
            write_csv_table(output_dir / "characterization_summary.csv", characterization_rows)
        group_summaries = summarize_group_behaviors(behavior_details, cfg.get("steady_state")) if behavior_details else None
        behavior_artifacts = (
            write_behavior_outputs(output_dir, behavior_summaries, group_summaries=group_summaries)
            if behavior_summaries
            else None
        )
        write_elapsed += time.perf_counter() - t_csv0
        elapsed = time.perf_counter() - t0
        run_summary = {
            "mode": "serial",
            "nproc": 1,
            "elapsed_sec": elapsed,
            "timing_breakdown": {
                "load_elapsed_sec": float(load_elapsed),
                "compute_elapsed_sec": float(compute_elapsed),
                "characterize_elapsed_sec": float(characterize_elapsed),
                "write_elapsed_sec": float(write_elapsed),
            },
            "parallel_metrics": {
                "work_unit": "sensor",
                "total_sensors": len(dataset.sensors),
                "assigned_sensors": len(dataset.sensors),
            },
        }
        if behavior_artifacts:
            run_summary["behavior_artifacts"] = behavior_artifacts
        save_json(output_dir / "run_summary.json", run_summary)
        print(
            json.dumps(
                {
                    "mode": "serial",
                    "elapsed_sec": elapsed,
                    "timing_breakdown": run_summary["timing_breakdown"],
                    "output_dir": str(output_dir),
                    "run_summary_json": str(output_dir / "run_summary.json"),
                    "cleaning_summary_csv": str(output_dir / "cleaning_summary.csv"),
                    "characterization_summary_csv": str(output_dir / "characterization_summary.csv") if characterization_rows else None,
                    **(behavior_artifacts or {}),
                },
                indent=2,
            )
        )
        return

    if args.mode == "replicated":
        summary = run_replicated_mode(
            args.cache_dir,
            output_dir,
            cfg["cleaning"],
            cfg["characterization"],
            behavior_cfg=cfg.get("steady_state"),
            do_characterize=args.characterize,
        )
    else:
        summary = run_partitioned_mode(
            args.cache_dir,
            output_dir,
            cfg["cleaning"],
            cfg["characterization"],
            behavior_cfg=cfg.get("steady_state"),
            do_characterize=args.characterize,
        )

    elapsed = time.perf_counter() - t0
    if rank == 0:
        # Only rank 0 writes the summary JSON to avoid concurrent writes from
        # multiple ranks pointing at the same output path.
        summary["elapsed_sec"] = elapsed
        save_json(output_dir / "run_summary.json", summary)
        print(
            json.dumps(
                {
                    "output_dir": str(output_dir),
                    "run_summary_json": str(output_dir / "run_summary.json"),
                    "cleaning_summary_csv": str(output_dir / "cleaning_summary.csv"),
                    "characterization_summary_csv": str(output_dir / "characterization_summary.csv") if args.characterize else None,
                    **summary,
                },
                indent=2,
            )
        )
    if comm is not None:
        # An explicit barrier/finalize keeps MPI teardown predictable on this
        # cluster stack and avoids ranks exiting at different times.
        comm.barrier()
        finalize_mpi()


def cmd_characterize(args, cfg, session):
    """Characterize cached signals without running the cleaning stage."""
    cfg = _apply_cli_overrides(cfg, args)
    dataset = load_sensor_cache(args.cache_dir, mmap_mode="r")
    out_dir = ensure_dir(Path(session["paths"]["outputs_dir"]) / "characterization")
    rows = []
    behavior_summaries: dict[str, dict] = {}
    behavior_details: dict[str, dict] = {}
    for sensor, values in dataset.sensors.items():
        char = characterize_signal(dataset.time, values, **cfg["characterization"])
        behavior, behavior_detail = analyze_signal_behavior(dataset.time, values, cfg.get("steady_state"))
        with open(out_dir / f"{normalize_header(sensor)}_characterization.json", "w", encoding="utf-8") as f:
            json.dump(char, f)
        rows.append(
            {
                "sensor": sensor,
                "method": char["method"],
                "n_dense": len(char["dense_time"]),
                "rate_of_change_plot": plot_rate_of_change(
                    dataset.time,
                    values,
                    out_dir / f"{normalize_header(sensor)}_rate_of_change.png",
                    title=f"{sensor}: signal and rate of change",
                    steady_segments=behavior.get("steady_segments"),
                ),
                "steady_state_summary": behavior["summary_text"],
            }
        )
        behavior_summaries[sensor] = behavior
        behavior_details[sensor] = behavior_detail
    write_csv_table(out_dir / "characterization_summary.csv", rows)
    group_summaries = summarize_group_behaviors(behavior_details, cfg.get("steady_state"))
    behavior_artifacts = write_behavior_outputs(out_dir, behavior_summaries, group_summaries=group_summaries)
    print(
        json.dumps(
            {
                "output_dir": str(out_dir),
                "characterized_sensors": len(rows),
                "characterization_summary_csv": str(out_dir / "characterization_summary.csv"),
                **(behavior_artifacts or {}),
            },
            indent=2,
        )
    )


def cmd_synth(args, cfg, session):
    """Generate a synthetic file for tests and scaling studies."""
    result = generate_synthetic_timeseries(
        args.out,
        n_rows=args.n_rows,
        n_sensors=args.n_sensors,
        noise_sigma=args.noise_sigma,
        spike_fraction=args.spike_fraction,
        seed=args.seed,
        time_mode=args.time_mode,
        header_style=args.header_style,
        include_junk_columns=args.include_junk_columns,
        flat_fraction=args.flat_fraction,
        dropout_fraction=args.dropout_fraction,
        output_format=args.output_format,
    )
    print(json.dumps({"synthetic_path": str(result["path"]), "metadata": result["metadata"]}, indent=2))


def cmd_workflow(args, cfg, session):
    """Run the consolidated high-level workflow command."""
    cfg = _apply_cli_overrides(cfg, args)
    report = run_workflow(args, cfg, session)
    print(json.dumps(report, indent=2))


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments, create a session, and dispatch the command."""
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
