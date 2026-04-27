#!/bin/bash
# File description:
# Batch-job template for launching the corrected mpirun-based strong-scaling
# runner on a cluster allocation. Replace the module lines and input path as
# needed.
#
#SBATCH -J ncdt_scaling
#SBATCH -N 1
#SBATCH -n 128
#SBATCH -t 03:00:00
#SBATCH -o analysis/slurm-%j.out
#SBATCH -e analysis/slurm-%j.err

set -euo pipefail

# FIXME: set the project root for your cluster checkout.
PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"

# FIXME: load your site modules here.
# module load python3/3.11
# module load openmpi

cd "$PROJECT_ROOT"

# FIXME: activate the correct virtual environment or Python environment.
source .venv/bin/activate

# FIXME: generate the campaign ahead of time or point this at a chosen raw or synthetic dataset.
INPUT_PATH="${1:-analysis/synthetic_campaigns/scaling_default/datasets/scaling_default_rows_100000_sensors_8_seed_100.csv}"

# FIXME: for raw-data-to-cleaned/spline outputs, prefer:
# python -m ncdt_cleaner.cli workflow "$INPUT_PATH" --characterize
#
# FIXME: for the MPI scaling study, use the standalone scaling runner below.
python benchmarks/run_scaling.py \
  --input-file "$INPUT_PATH" \
  --process-counts 1 2 4 8 16 24 48 64 96 128 \
  --repeat 1 \
  --mpi-launcher mpirun

# FIXME: inspect benchmark_manifest.json, timing_breakdown_summary.csv,
# parallel_metrics_summary.csv, and logs/ under the output dir.
