"""
Rebuild benchmark summaries and plots from an existing benchmark_results.json.

This is useful when a long benchmark run finishes its measurements but fails
while exporting a derived CSV, figure, or manifest.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ncdt_cleaner.benchmarks import write_benchmark_results


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild benchmark outputs from benchmark_results.json")
    parser.add_argument("results_json", help="Path to an existing benchmark_results.json file")
    parser.add_argument("--out-dir", default=None, help="Directory to rewrite summaries and plots into")
    args = parser.parse_args()

    results_path = Path(args.results_json).resolve()
    payload = json.loads(results_path.read_text(encoding="utf-8"))
    rows = payload.get("results", [])
    out_dir = Path(args.out_dir).resolve() if args.out_dir is not None else results_path.parent
    artifacts = write_benchmark_results(out_dir, rows)
    print(json.dumps({"out_dir": str(out_dir), "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
