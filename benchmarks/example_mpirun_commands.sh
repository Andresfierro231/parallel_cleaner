#!/usr/bin/env bash
# File description:
# Minimal example showing the preferred workflow command for benchmark users.
# New users can copy this script pattern instead of running the old manual
# sequence of inspect, cache-build, clean, and benchmark commands by hand.

set -euo pipefail

INPUT_PATH="${1:-analysis/synth_2e6_16s.csv}"

python -m ncdt_cleaner.cli workflow "$INPUT_PATH" \
  --modes serial replicated partitioned \
  --process-counts 2 4 8
