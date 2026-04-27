#!/bin/bash
# File description:
# Run the weak-scaling analysis used to strengthen the MPI discussion in the paper.

#SBATCH -J ncdt-weak-scale
#SBATCH -A ASC23046
#SBATCH -p NuclearEnergy
#SBATCH -N 1
#SBATCH -n 24
#SBATCH -t 03:00:00
#SBATCH -o analysis/slurm-ncdt-weak-scale-%j.out
#SBATCH -e analysis/slurm-ncdt-weak-scale-%j.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CONFIG="${CONFIG:-configs/default_config.json}"
MPI_LAUNCHER="${MPI_LAUNCHER:-mpirun}"
CAMPAIGN_NAME="${CAMPAIGN_NAME:-weak_scaling_parallel_report}"
OUT_DIR="${OUT_DIR:-analysis/weak_scaling/weak_scaling_parallel_report}"
PROCESS_COUNTS="${PROCESS_COUNTS:-1 2 4 8 16 24}"
ROWS_PER_RANK="${ROWS_PER_RANK:-125000}"
NUM_SENSORS="${NUM_SENSORS:-8}"
REPEAT="${REPEAT:-1}"
MODES="${MODES:-replicated partitioned}"

read -r -a PROCESS_COUNTS_ARR <<< "$PROCESS_COUNTS"
read -r -a MODES_ARR <<< "$MODES"

cd "$PROJECT_ROOT"
source .venv/bin/activate

python benchmarks/run_weak_scaling.py \
  --config "$CONFIG" \
  --campaign-name "$CAMPAIGN_NAME" \
  --out-dir "$OUT_DIR" \
  --process-counts "${PROCESS_COUNTS_ARR[@]}" \
  --rows-per-rank "$ROWS_PER_RANK" \
  --num-sensors "$NUM_SENSORS" \
  --repeat "$REPEAT" \
  --modes "${MODES_ARR[@]}" \
  --mpi-launcher "$MPI_LAUNCHER"
