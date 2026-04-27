#!/usr/bin/env bash
# File description:
# Minimal example showing the preferred workflow command for benchmark users.
# New users can copy this script pattern instead of running the old manual
# sequence of inspect, cache-build, clean, and benchmark commands by hand.

set -euo pipefail

# Example 1: one centralized command from raw noisy input to cleaned outputs,
# optional spline characterization, and benchmark artifacts.
RAW_INPUT="${1:-/path/to/noisy_sensor_data.csv}"
python -m ncdt_cleaner.cli workflow "$RAW_INPUT" \
  --modes serial replicated partitioned \
  --process-counts 2 4 8 16 \
  --characterize

# Example 2: generate a reproducible strong-scaling campaign when the real
# input is too small for a meaningful MPI study.
python benchmarks/make_synthetic_campaign.py \
  --campaign-name scaling_default \
  --rows 100000 500000 1000000  \
  --num-sensors 8

SYNTHETIC_INPUT="${2:-analysis/synthetic_campaigns/scaling_default/datasets/scaling_default_rows_100000_sensors_8_seed_100.csv}"

# Example 3: corrected strong-scaling study with per-repeat logs, timing
# breakdowns, and parallel-metrics summaries.
python benchmarks/run_scaling.py \
  --input-file "$SYNTHETIC_INPUT" \
  --modes serial replicated partitioned \
  --process-counts 2 4 8 16 \
  --mpi-launcher mpirun

# Example 4: weak-scaling study with work per rank held approximately constant.
python benchmarks/run_weak_scaling.py \
  --campaign-name weak_scaling_example \
  --process-counts 1 2 4 8 16 \
  --rows-per-rank 125000 \
  --modes replicated partitioned \
  --mpi-launcher mpirun
