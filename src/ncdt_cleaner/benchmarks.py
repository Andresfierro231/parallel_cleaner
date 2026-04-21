'''
File description:
Benchmark orchestration helpers for serial and MPI scaling experiments.

The functions in this module build command lines, run repeated timing studies,
and write summary tables and plots that can be dropped into the course paper.
'''

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from .config import save_json
from .stats import write_csv_table
from .utils import ensure_dir

MODE_ORDER = ("serial", "replicated", "partitioned")


def benchmark_subprocess(command: list[str], repeat: int = 3) -> dict:
    """Run one command multiple times and keep the full timing history."""
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        subprocess.run(command, check=True)
        times.append(time.perf_counter() - t0)
    return {
        "command": command,
        "elapsed_sec": min(times),
        "repeat": repeat,
        "all_times": times,
    }


def clean_cli_command(
    config_path: str,
    cache_dir: str | Path,
    mode: str,
    characterize: bool = False,
    python_executable: str | None = None,
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
) -> list[dict]:
    """Run the requested serial and MPI timing study and return raw rows."""
    selected_modes = [mode for mode in MODE_ORDER if mode in set(modes or MODE_ORDER)]
    rows: list[dict] = []

    if "serial" in selected_modes:
        serial_command = clean_cli_command(
            config_path=config_path,
            cache_dir=cache_dir,
            mode="serial",
            characterize=characterize,
            python_executable=python_executable,
        )
        result = benchmark_subprocess(serial_command, repeat=repeat)
        rows.append(
            {
                "mode": "serial",
                "nproc": 1,
                "elapsed_sec": result["elapsed_sec"],
                "all_times": result["all_times"],
                "command": result["command"],
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
                ),
                nproc=nproc,
                mpi_launcher=mpi_launcher,
            )
            result = benchmark_subprocess(command, repeat=repeat)
            rows.append(
                {
                    "mode": mode,
                    "nproc": nproc,
                    "elapsed_sec": result["elapsed_sec"],
                    "all_times": result["all_times"],
                    "command": result["command"],
                }
            )
    return rows


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


def write_benchmark_results(out_dir: str | Path, rows: list[dict]) -> dict:
    """Write raw benchmark results, summaries, and plots into one directory."""
    from .plotting import plot_speedup

    out_dir = ensure_dir(out_dir)
    results_json = Path(out_dir) / "benchmark_results.json"
    summary_json = Path(out_dir) / "benchmark_summary.json"
    summary_csv = Path(out_dir) / "benchmark_summary.csv"
    save_json(results_json, {"results": rows})
    summary_rows = summarize_benchmark_rows(rows)
    save_json(summary_json, {"summary": summary_rows})
    write_csv_table(summary_csv, summary_rows)
    plots = plot_speedup(rows, Path(out_dir) / "plots")
    return {
        "results_json": str(results_json),
        "summary_json": str(summary_json),
        "summary_csv": str(summary_csv),
        "plots": plots,
    }
