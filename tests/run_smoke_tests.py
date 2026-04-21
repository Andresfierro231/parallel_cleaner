from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    sample_out = root / "analysis" / "synthetic_smoke.csv"
    subprocess.run([sys.executable, "-m", "ncdt_cleaner.cli", "synth", "--out", str(sample_out), "--n-rows", "1000", "--n-sensors", "4"], check=True)
    subprocess.run([sys.executable, "-m", "ncdt_cleaner.cli", "inspect", str(sample_out)], check=True)
    subprocess.run([sys.executable, "-m", "ncdt_cleaner.cli", "cache-build", str(sample_out)], check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
