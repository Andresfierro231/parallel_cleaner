#!/bin/bash
# File description:
# Run one detailed strong-scaling benchmark with internal MPI timing breakdowns.

#SBATCH -J ncdt-strong-diag
#SBATCH -A ASC23046
#SBATCH -p NuclearEnergy
#SBATCH -N 1
#SBATCH -n 128
#SBATCH -t 03:00:00
#SBATCH -o analysis/slurm-ncdt-strong-diag-%j.out
#SBATCH -e analysis/slurm-ncdt-strong-diag-%j.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CONFIG="${CONFIG:-configs/default_config.json}"
MPI_LAUNCHER="${MPI_LAUNCHER:-mpirun}"
REPEAT="${REPEAT:-1}"
DATASET_PATH="${DATASET_PATH:-analysis/synthetic_campaigns/paper_scaling_100k_1m/datasets/paper_scaling_100k_1m_rows_1000000_sensors_8_seed_103.csv}"
OUT_DIR="${OUT_DIR:-analysis/parallel_diagnostics/strong_scaling_1m_detailed}"
PROCESS_COUNTS="${PROCESS_COUNTS:-1 2 4 8 16 24 48 64 96 128}"
MODES="${MODES:-serial replicated partitioned}"

read -r -a PROCESS_COUNTS_ARR <<< "$PROCESS_COUNTS"
read -r -a MODES_ARR <<< "$MODES"

cd "$PROJECT_ROOT"
source .venv/bin/activate

python benchmarks/run_scaling.py \
  --input-file "$DATASET_PATH" \
  --config "$CONFIG" \
  --modes "${MODES_ARR[@]}" \
  --process-counts "${PROCESS_COUNTS_ARR[@]}" \
  --repeat "$REPEAT" \
  --mpi-launcher "$MPI_LAUNCHER" \
  --out-dir "$OUT_DIR"
