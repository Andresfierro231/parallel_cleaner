'''
File description:
Benchmark orchestration helpers for serial and MPI scaling experiments.

The functions in this module build command lines, run repeated timing studies,
and write summary tables and plots that can be dropped into the course paper.
'''

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

from .config import save_json
from .stats import write_csv_table
from .utils import ensure_dir

MODE_ORDER = ("serial", "replicated", "partitioned")


def benchmark_subprocess(
    command: list[str],
    repeat: int = 3,
    *,
    log_dir: str | Path | None = None,
    log_prefix: str | None = None,
) -> dict:
    """Run one command multiple times and keep the full timing history."""
    log_dir = ensure_dir(log_dir) if log_dir is not None else None
    times = []
    child_payloads = []
    stdout_logs = []
    stderr_logs = []
    for _ in range(repeat):
        repeat_idx = len(times) + 1
        t0 = time.perf_counter()
        proc = subprocess.run(command, check=True, capture_output=True, text=True)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        payload = None
        if stdout.strip():
            try:
                payload = json.loads(stdout)
            except json.JSONDecodeError:
                payload = None
        child_payloads.append(payload)
        if log_dir is not None and log_prefix is not None:
            stdout_path = Path(log_dir) / f"{log_prefix}_repeat{repeat_idx}.out"
            stderr_path = Path(log_dir) / f"{log_prefix}_repeat{repeat_idx}.err"
            stdout_path.write_text(stdout, encoding="utf-8")
            stderr_path.write_text(stderr, encoding="utf-8")
            stdout_logs.append(str(stdout_path))
            stderr_logs.append(str(stderr_path))
    return {
        "command": command,
        "elapsed_sec": min(times),
        "repeat": repeat,
        "all_times": times,
        "child_payloads": child_payloads,
        "stdout_logs": stdout_logs,
        "stderr_logs": stderr_logs,
    }


def clean_cli_command(
    config_path: str,
    cache_dir: str | Path,
    mode: str,
    characterize: bool = False,
    python_executable: str | None = None,
    steady_window_seconds: float | None = None,
) -> list[str]:
    """Build the canonical `clean` CLI command for one execution mode."""
    command = [
        python_executable or sys.executable,
        "-m",
        "ncdt_cleaner.cli",
        "--config",
        config_path,
        "clean",
        "--cache-dir",
        str(cache_dir),
        "--mode",
        mode,
    ]
    if characterize:
        command.append("--characterize")
    if steady_window_seconds is not None:
        command.extend(["--steady-window-seconds", str(float(steady_window_seconds))])
    return command


def mpi_wrapped_command(command: list[str], nproc: int, mpi_launcher: str = "mpirun") -> list[str]:
    """Prefix a normal Python command with the chosen MPI launcher."""
    return [mpi_launcher, "-n", str(nproc), *command]


def run_scaling_study(
    cache_dir: str | Path,
    config_path: str,
    repeat: int,
    process_counts: list[int],
    modes: list[str] | tuple[str, ...] | None = None,
    characterize: bool = False,
    python_executable: str | None = None,
    mpi_launcher: str = "mpirun",
    log_dir: str | Path | None = None,
    steady_window_seconds: float | None = None,
) -> list[dict]:
    """Run the requested serial and MPI timing study and return raw rows."""
    selected_modes = [mode for mode in MODE_ORDER if mode in set(modes or MODE_ORDER)]
    rows: list[dict] = []
    log_dir = ensure_dir(log_dir if log_dir is not None else Path(cache_dir).parent / "benchmark_logs")

    if "serial" in selected_modes:
        serial_command = clean_cli_command(
            config_path=config_path,
            cache_dir=cache_dir,
            mode="serial",
            characterize=characterize,
            python_executable=python_executable,
            steady_window_seconds=steady_window_seconds,
        )
        result = benchmark_subprocess(serial_command, repeat=repeat, log_dir=log_dir, log_prefix="serial_n1")
        rows.append(
            {
                "mode": "serial",
                "nproc": 1,
                "elapsed_sec": result["elapsed_sec"],
                "all_times": result["all_times"],
                "command": result["command"],
                "child_payloads": result["child_payloads"],
                "stdout_logs": result["stdout_logs"],
                "stderr_logs": result["stderr_logs"],
            }
        )

    for mode in selected_modes:
        if mode == "serial":
            continue
        for nproc in process_counts:
            if nproc <= 1:
                continue
            # MPI timings are kept as raw rows first; report-oriented summaries
            # are derived later so the original measurements remain available.
            command = mpi_wrapped_command(
                clean_cli_command(
                    config_path=config_path,
                    cache_dir=cache_dir,
                    mode=mode,
                    characterize=characterize,
                    python_executable=python_executable,
                    steady_window_seconds=steady_window_seconds,
                ),
                nproc=nproc,
                mpi_launcher=mpi_launcher,
            )
            result = benchmark_subprocess(
                command,
                repeat=repeat,
                log_dir=log_dir,
                log_prefix=f"{mode}_n{nproc}",
            )
            rows.append(
                {
                    "mode": mode,
                    "nproc": nproc,
                    "elapsed_sec": result["elapsed_sec"],
                    "all_times": result["all_times"],
                    "command": result["command"],
                    "child_payloads": result["child_payloads"],
                    "stdout_logs": result["stdout_logs"],
                    "stderr_logs": result["stderr_logs"],
                }
            )
    return rows


def _mean_numeric(values: list[float | int]) -> float | None:
    """Return the arithmetic mean for a non-empty numeric list."""
    if not values:
        return None
    return float(sum(float(value) for value in values) / len(values))


def summarize_benchmark_rows(rows: list[dict]) -> list[dict]:
    """Convert raw timing rows into speedup and efficiency summary rows."""
    serial_row = next((row for row in rows if row["mode"] == "serial" and int(row["nproc"]) == 1), None)
    serial_elapsed = float(serial_row["elapsed_sec"]) if serial_row else None

    ordered_rows = sorted(
        rows,
        key=lambda row: (
            MODE_ORDER.index(row["mode"]) if row["mode"] in MODE_ORDER else len(MODE_ORDER),
            int(row["nproc"]),
        ),
    )
    summary_rows = []
    for row in ordered_rows:
        all_times = [float(value) for value in row.get("all_times", [])]
        speedup = None if serial_elapsed is None else serial_elapsed / float(row["elapsed_sec"])
        efficiency = None if speedup is None else speedup / max(int(row["nproc"]), 1)
        summary_rows.append(
            {
                "mode": row["mode"],
                "nproc": int(row["nproc"]),
                "elapsed_sec": float(row["elapsed_sec"]),
                "repeat": len(all_times),
                "mean_elapsed_sec": sum(all_times) / len(all_times) if all_times else float(row["elapsed_sec"]),
                "max_elapsed_sec": max(all_times) if all_times else float(row["elapsed_sec"]),
                "speedup_vs_serial": speedup,
                "parallel_efficiency": efficiency,
            }
        )
    return summary_rows


def summarize_timing_breakdowns(rows: list[dict]) -> list[dict]:
    """Aggregate internal stage timings emitted by child clean runs."""
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            MODE_ORDER.index(row["mode"]) if row["mode"] in MODE_ORDER else len(MODE_ORDER),
            int(row["nproc"]),
        ),
    )
    summary = []
    for row in ordered_rows:
        payloads = [payload for payload in row.get("child_payloads", []) if isinstance(payload, dict)]
        breakdowns = [payload.get("timing_breakdown") for payload in payloads if isinstance(payload.get("timing_breakdown"), dict)]
        if not breakdowns:
            continue
        keys = sorted({key for breakdown in breakdowns for key in breakdown})
        result = {
            "mode": row["mode"],
            "nproc": int(row["nproc"]),
            "repeat": len(breakdowns),
        }
        for key in keys:
            values = [float(breakdown[key]) for breakdown in breakdowns if breakdown.get(key) is not None]
            result[f"mean_{key}"] = _mean_numeric(values)
            result[f"max_{key}"] = max(values) if values else None
        summary.append(result)
    return summary


def summarize_parallel_metrics(rows: list[dict]) -> list[dict]:
    """Aggregate work-distribution metadata emitted by child clean runs."""
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            MODE_ORDER.index(row["mode"]) if row["mode"] in MODE_ORDER else len(MODE_ORDER),
            int(row["nproc"]),
        ),
    )
    summary = []
    for row in ordered_rows:
        payloads = [payload for payload in row.get("child_payloads", []) if isinstance(payload, dict)]
        metrics_rows = [payload.get("parallel_metrics") for payload in payloads if isinstance(payload.get("parallel_metrics"), dict)]
        if not metrics_rows:
            continue
        keys = sorted({key for metrics in metrics_rows for key in metrics})
        result = {
            "mode": row["mode"],
            "nproc": int(row["nproc"]),
            "repeat": len(metrics_rows),
        }
        for key in keys:
            values = [metrics.get(key) for metrics in metrics_rows if metrics.get(key) is not None]
            if not values:
                result[key] = None
            elif isinstance(values[0], (int, float)):
                result[f"mean_{key}"] = _mean_numeric([float(value) for value in values])
                result[f"max_{key}"] = max(float(value) for value in values)
            else:
                result[key] = values[0]
        summary.append(result)
    return summary


def write_benchmark_results(out_dir: str | Path, rows: list[dict]) -> dict:
    """Write raw benchmark results, summaries, and plots into one directory."""
    from .plotting import plot_speedup

    out_dir = ensure_dir(out_dir)
    manifest_json = Path(out_dir) / "benchmark_manifest.json"
    results_json = Path(out_dir) / "benchmark_results.json"
    summary_json = Path(out_dir) / "benchmark_summary.json"
    summary_csv = Path(out_dir) / "benchmark_summary.csv"
    breakdown_json = Path(out_dir) / "timing_breakdown_summary.json"
    breakdown_csv = Path(out_dir) / "timing_breakdown_summary.csv"
    metrics_json = Path(out_dir) / "parallel_metrics_summary.json"
    metrics_csv = Path(out_dir) / "parallel_metrics_summary.csv"
    save_json(results_json, {"results": rows})
    summary_rows = summarize_benchmark_rows(rows)
    breakdown_rows = summarize_timing_breakdowns(rows)
    metrics_rows = summarize_parallel_metrics(rows)
    save_json(summary_json, {"summary": summary_rows})
    write_csv_table(summary_csv, summary_rows)
    save_json(breakdown_json, {"timing_breakdown_summary": breakdown_rows})
    write_csv_table(breakdown_csv, breakdown_rows)
    save_json(metrics_json, {"parallel_metrics_summary": metrics_rows})
    write_csv_table(metrics_csv, metrics_rows)
    plots = plot_speedup(rows, Path(out_dir) / "plots")
    manifest = {
        "modes": [row["mode"] for row in rows],
        "process_counts": sorted({int(row["nproc"]) for row in rows}),
        "repeat": max((len(row.get("all_times", [])) for row in rows), default=0),
        "row_count": len(rows),
        "has_timing_breakdown": bool(breakdown_rows),
        "has_parallel_metrics": bool(metrics_rows),
        "stdout_logs": [path for row in rows for path in row.get("stdout_logs", [])],
        "stderr_logs": [path for row in rows for path in row.get("stderr_logs", [])],
    }
    save_json(manifest_json, manifest)
    return {
        "manifest_json": str(manifest_json),
        "results_json": str(results_json),
        "summary_json": str(summary_json),
        "summary_csv": str(summary_csv),
        "timing_breakdown_json": str(breakdown_json),
        "timing_breakdown_csv": str(breakdown_csv),
        "parallel_metrics_json": str(metrics_json),
        "parallel_metrics_csv": str(metrics_csv),
        "plots": plots,
    }
