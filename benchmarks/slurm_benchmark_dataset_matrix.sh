#!/bin/bash
# File description:
# Run one full benchmark matrix for a single dataset produced by the paper
# scaling campaign.

#SBATCH -J ncdt-paper-benchmark
#SBATCH -A ASC23046
#SBATCH -p NuclearEnergy
#SBATCH -N 1
#SBATCH -n 128
#SBATCH -t 02:00:00
#SBATCH -o analysis/slurm-ncdt-paper-benchmark-%j.out
#SBATCH -e analysis/slurm-ncdt-paper-benchmark-%j.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CONFIG="${CONFIG:-configs/default_config.json}"
MPI_LAUNCHER="${MPI_LAUNCHER:-mpirun}"
REPEAT="${REPEAT:-3}"
PROCESS_COUNTS="${PROCESS_COUNTS:-1 2 4 8 16 24 48 64 96 128}"
MODES="${MODES:-serial replicated partitioned}"
DATASET_PATH="${DATASET_PATH:?DATASET_PATH must be set}"
OUT_DIR="${OUT_DIR:?OUT_DIR must be set}"

read -r -a PROCESS_COUNTS_ARR <<< "$PROCESS_COUNTS"
read -r -a MODES_ARR <<< "$MODES"

cd "$PROJECT_ROOT"
source .venv/bin/activate

echo "[$(date --iso-8601=seconds)] Benchmarking ${DATASET_PATH} with ${MPI_LAUNCHER}"
python benchmarks/run_scaling.py \
  --input-file "$DATASET_PATH" \
  --config "$CONFIG" \
  --modes "${MODES_ARR[@]}" \
  --process-counts "${PROCESS_COUNTS_ARR[@]}" \
  --repeat "$REPEAT" \
  --mpi-launcher "$MPI_LAUNCHER" \
  --out-dir "$OUT_DIR"

echo "[$(date --iso-8601=seconds)] Results written to ${OUT_DIR}"
