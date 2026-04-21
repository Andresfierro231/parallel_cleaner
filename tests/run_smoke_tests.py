'''
File description:
Very small end-to-end smoke test for the CLI and workflow command.

The goal is not exhaustive testing. Instead, this script quickly verifies that
the main commands still execute in sequence on a synthetic dataset.
'''

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Run a minimal end-to-end smoke test on synthetic data."""
    root = Path(__file__).resolve().parents[1]
    sample_out = root / "analysis" / "synthetic_smoke.csv"
    subprocess.run([sys.executable, "-m", "ncdt_cleaner.cli", "synth", "--out", str(sample_out), "--n-rows", "1000", "--n-sensors", "4"], check=True, cwd=root)
    subprocess.run([sys.executable, "-m", "ncdt_cleaner.cli", "inspect", str(sample_out)], check=True, cwd=root)
    subprocess.run([sys.executable, "-m", "ncdt_cleaner.cli", "cache-build", str(sample_out)], check=True, cwd=root)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ncdt_cleaner.cli",
            "workflow",
            str(sample_out),
            "--modes",
            "serial",
            "--skip-benchmark",
        ],
        check=True,
        cwd=root,
    )
    subprocess.run(
        [
            sys.executable,
            "benchmarks/run_scaling.py",
            "--input-file",
            str(sample_out),
            "--modes",
            "serial",
            "--repeat",
            "1",
        ],
        check=True,
        cwd=root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
