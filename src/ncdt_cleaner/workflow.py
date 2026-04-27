'''
File description:
High-level workflow orchestration for the end-to-end paper-oriented pipeline.

This module is the glue that reduces the repo's previous manual steps into one
command that can inspect data, build cache files, run analysis modes, and emit
benchmark artifacts for the report.
'''

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from .benchmarks import MODE_ORDER, clean_cli_command, mpi_wrapped_command, run_scaling_study, write_benchmark_results
from .cache import ensure_cache_for_input
from .config import save_json
from .inspectors import inspect_file
from .utils import ensure_dir


def _run_json_command(command: list[str]) -> dict[str, Any]:
    """Execute a child command and parse the JSON object it prints."""
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
    """Choose a representative MPI size for the one-off analysis runs."""
    if requested_nproc is not None:
        return requested_nproc
    mpi_counts = [count for count in process_counts if count > 1]
    return max(mpi_counts) if mpi_counts else 1


def _selected_modes(requested_modes: list[str] | None) -> list[str]:
    """Return modes in a stable report-friendly order."""
    modes = set(requested_modes or MODE_ORDER)
    return [mode for mode in MODE_ORDER if mode in modes]


def _clean_run_artifacts(payload: dict[str, Any]) -> dict[str, Any]:
    """Augment a clean command payload with the artifact paths users expect."""
    output_dir = payload.get("output_dir")
    artifacts = {
        "output_dir": output_dir,
        "run_summary_json": str(Path(output_dir) / "run_summary.json") if output_dir else None,
        "cleaning_summary_csv": str(Path(output_dir) / "cleaning_summary.csv") if output_dir else None,
        "characterization_summary_csv": str(Path(output_dir) / "characterization_summary.csv") if output_dir else None,
        "steady_state_summary_csv": str(Path(output_dir) / "steady_state_summary.csv") if output_dir else None,
        "steady_state_groups_summary_csv": str(Path(output_dir) / "steady_state_groups_summary.csv") if output_dir else None,
        "steady_state_report_md": str(Path(output_dir) / "steady_state_report.md") if output_dir else None,
    }
    artifacts.update(payload)
    return artifacts


def run_workflow(args, cfg: dict, session: dict) -> dict[str, Any]:
    """Run the end-to-end workflow and write a consolidated report JSON."""
    selected_modes = _selected_modes(args.modes)
    process_counts = [int(value) for value in (args.process_counts or cfg["benchmark"]["process_counts"])]
    repeat = int(args.repeat or cfg["benchmark"]["repeat"])
    workflow_dir = ensure_dir(Path(session["paths"]["outputs_dir"]) / "workflow")

    if args.cache_dir is None and args.input is None:
        raise ValueError("workflow requires either an input file or --cache-dir")

    inspection_report = None
    normalized_summary = None
    cache_resolution = None

    cache_dir = Path(args.cache_dir) if args.cache_dir else Path(session["paths"]["cache_dir"])
    if args.input is not None:
        if not args.skip_inspect:
            # Inspection is intentionally cheap and helps new users verify the
            # input before cache generation changes anything on disk.
            reports = [inspect_file(args.input, cfg)]
            inspection_report = workflow_dir / "inspection_report.json"
            save_json(inspection_report, {"files": reports})

        # Prefer reusing a compatible cache so benchmark jobs can start directly
        # from a raw input file without manual cache-dir copying.
        cache_resolution = ensure_cache_for_input(
            input_path=args.input,
            config_path=args.config,
            config=cfg,
            analysis_dir=cfg["analysis_dir"],
            sheet_name=args.sheet_name,
            target_cache_dir=cache_dir,
            target_session_dir=session["paths"]["session_dir"],
        )
        cache_dir = Path(cache_resolution["cache_dir"])
        if cache_resolution.get("summary") is not None:
            normalized_summary = workflow_dir / "normalized_summary.json"
            save_json(normalized_summary, cache_resolution["summary"])

    elif args.cache_dir is not None:
        cache_resolution = {
            "status": "provided",
            "cache_dir": str(Path(args.cache_dir).resolve()),
            "cache_metadata_path": None,
            "matched_session_dir": None,
        }

    cache_dir = cache_dir.resolve()
    analysis_nproc = _analysis_nproc(process_counts, args.analysis_nproc)
    steady_window_seconds = args.steady_window_seconds

    clean_runs = []
    if not args.skip_clean_runs:
        for mode in selected_modes:
            # These one-off analysis runs create concrete outputs that the user
            # can inspect directly, separate from the repeated benchmark runs.
            command = clean_cli_command(
                config_path=args.config,
                cache_dir=cache_dir,
                mode=mode,
                characterize=args.characterize,
                python_executable=sys.executable,
                steady_window_seconds=steady_window_seconds,
            )
            nproc = 1
            if mode != "serial":
                nproc = analysis_nproc
                command = mpi_wrapped_command(command, nproc=nproc, mpi_launcher=args.mpi_launcher)
            payload = _run_json_command(command)
            clean_runs.append(_clean_run_artifacts({"mode": mode, "nproc": nproc, **payload}))

    benchmark = None
    if not args.skip_benchmark:
        # Benchmarks are kept separate from the one-off analysis runs so the
        # paper artifacts reflect repeated timing measurements, not setup work.
        benchmark_rows = run_scaling_study(
            cache_dir=cache_dir,
            config_path=args.config,
            repeat=repeat,
            process_counts=process_counts,
            modes=selected_modes,
            characterize=args.characterize,
            python_executable=sys.executable,
            mpi_launcher=args.mpi_launcher,
            log_dir=workflow_dir / "benchmark" / "logs",
            steady_window_seconds=steady_window_seconds,
        )
        benchmark_dir = ensure_dir(workflow_dir / "benchmark")
        benchmark = write_benchmark_results(benchmark_dir, benchmark_rows)

    report = {
        "workflow_dir": str(workflow_dir),
        "cache_dir": str(cache_dir),
        "cache_resolution": cache_resolution,
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
            "reference_steady_state_summary_csv": next(
                (run["steady_state_summary_csv"] for run in clean_runs if run.get("steady_state_summary_csv")),
                None,
            ),
            "reference_steady_state_groups_summary_csv": next(
                (run["steady_state_groups_summary_csv"] for run in clean_runs if run.get("steady_state_groups_summary_csv")),
                None,
            ),
            "reference_steady_state_report_md": next(
                (run["steady_state_report_md"] for run in clean_runs if run.get("steady_state_report_md")),
                None,
            ),
        },
    }
    save_json(workflow_dir / "workflow_report.json", report)
    return report
