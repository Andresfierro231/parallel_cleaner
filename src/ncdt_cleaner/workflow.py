from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from .benchmarks import MODE_ORDER, clean_cli_command, mpi_wrapped_command, run_scaling_study, write_benchmark_results
from .cache import write_sensor_cache
from .config import save_json
from .inspectors import inspect_file
from .normalization import dataframe_to_sensor_dataset
from .readers import read_tabular_file
from .utils import ensure_dir


def _run_json_command(command: list[str]) -> dict[str, Any]:
    proc = subprocess.run(command, check=True, capture_output=True, text=True)
    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError(f"Command produced no JSON output: {' '.join(command)}")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Command did not return parseable JSON output.\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{proc.stderr.strip()}"
        ) from exc


def _analysis_nproc(process_counts: list[int], requested_nproc: int | None) -> int:
    if requested_nproc is not None:
        return requested_nproc
    mpi_counts = [count for count in process_counts if count > 1]
    return max(mpi_counts) if mpi_counts else 1


def _selected_modes(requested_modes: list[str] | None) -> list[str]:
    modes = set(requested_modes or MODE_ORDER)
    return [mode for mode in MODE_ORDER if mode in modes]


def _clean_run_artifacts(payload: dict[str, Any]) -> dict[str, Any]:
    output_dir = payload.get("output_dir")
    artifacts = {
        "output_dir": output_dir,
        "run_summary_json": str(Path(output_dir) / "run_summary.json") if output_dir else None,
        "cleaning_summary_csv": str(Path(output_dir) / "cleaning_summary.csv") if output_dir else None,
    }
    artifacts.update(payload)
    return artifacts


def run_workflow(args, cfg: dict, session: dict) -> dict[str, Any]:
    selected_modes = _selected_modes(args.modes)
    process_counts = [int(value) for value in (args.process_counts or cfg["benchmark"]["process_counts"])]
    repeat = int(args.repeat or cfg["benchmark"]["repeat"])
    workflow_dir = ensure_dir(Path(session["paths"]["outputs_dir"]) / "workflow")

    if args.cache_dir is None and args.input is None:
        raise ValueError("workflow requires either an input file or --cache-dir")

    inspection_report = None
    normalized_summary = None

    cache_dir = Path(args.cache_dir) if args.cache_dir else Path(session["paths"]["cache_dir"])
    if args.input is not None:
        if not args.skip_inspect:
            reports = [inspect_file(args.input, cfg)]
            inspection_report = workflow_dir / "inspection_report.json"
            save_json(inspection_report, {"files": reports})

        if args.cache_dir is None:
            df = read_tabular_file(args.input, sheet_name=args.sheet_name)
            dataset, summary = dataframe_to_sensor_dataset(df, Path(args.input).stem, cfg)
            write_sensor_cache(dataset, cache_dir, dtype=cfg["cache"]["dtype"])
            normalized_summary = workflow_dir / "normalized_summary.json"
            save_json(normalized_summary, summary)

    cache_dir = cache_dir.resolve()
    analysis_nproc = _analysis_nproc(process_counts, args.analysis_nproc)

    clean_runs = []
    if not args.skip_clean_runs:
        for mode in selected_modes:
            command = clean_cli_command(
                config_path=args.config,
                cache_dir=cache_dir,
                mode=mode,
                characterize=args.characterize,
                python_executable=sys.executable,
            )
            nproc = 1
            if mode != "serial":
                nproc = analysis_nproc
                command = mpi_wrapped_command(command, nproc=nproc, mpi_launcher=args.mpi_launcher)
            payload = _run_json_command(command)
            clean_runs.append(_clean_run_artifacts({"mode": mode, "nproc": nproc, **payload}))

    benchmark = None
    if not args.skip_benchmark:
        benchmark_rows = run_scaling_study(
            cache_dir=cache_dir,
            config_path=args.config,
            repeat=repeat,
            process_counts=process_counts,
            modes=selected_modes,
            characterize=args.characterize,
            python_executable=sys.executable,
            mpi_launcher=args.mpi_launcher,
        )
        benchmark_dir = ensure_dir(workflow_dir / "benchmark")
        benchmark = write_benchmark_results(benchmark_dir, benchmark_rows)

    report = {
        "workflow_dir": str(workflow_dir),
        "cache_dir": str(cache_dir),
        "inspection_report": str(inspection_report) if inspection_report else None,
        "normalized_summary": str(normalized_summary) if normalized_summary else None,
        "clean_runs": clean_runs,
        "benchmark": benchmark,
        "paper_artifacts": {
            "benchmark_summary_csv": benchmark["summary_csv"] if benchmark else None,
            "benchmark_summary_json": benchmark["summary_json"] if benchmark else None,
            "benchmark_results_json": benchmark["results_json"] if benchmark else None,
            "reference_cleaning_summary_csv": next(
                (run["cleaning_summary_csv"] for run in clean_runs if run.get("cleaning_summary_csv")),
                None,
            ),
        },
    }
    save_json(workflow_dir / "workflow_report.json", report)
    return report
