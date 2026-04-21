#!/usr/bin/env bash
set -euo pipefail

INPUT_PATH="${1:-analysis/synth_2e6_16s.csv}"

python -m ncdt_cleaner.cli workflow "$INPUT_PATH" \
  --modes serial replicated partitioned \
  --process-counts 2 4 8
