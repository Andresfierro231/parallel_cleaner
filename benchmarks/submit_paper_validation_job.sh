#!/bin/bash
# File description:
# Submit a short validation benchmark to confirm that mpirun launches real
# multi-rank MPI jobs before queuing the full paper benchmark matrix.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CONFIG="${CONFIG:-configs/default_config.json}"
MPI_LAUNCHER="${MPI_LAUNCHER:-mpirun}"
CAMPAIGN_NAME="${CAMPAIGN_NAME:-paper_scaling_100k_1m}"
NUM_SENSORS="${NUM_SENSORS:-8}"
ROW_COUNT="${ROW_COUNT:-1000000}"
SEED="${SEED:-103}"
REPEAT="${REPEAT:-1}"
PROCESS_COUNTS="${PROCESS_COUNTS:-2 4 8}"
MODES="${MODES:-replicated partitioned}"
WALLTIME="${WALLTIME:-01:30:00}"

cd "$PROJECT_ROOT"

dataset_name="${CAMPAIGN_NAME}_rows_${ROW_COUNT}_sensors_${NUM_SENSORS}_seed_${SEED}"
dataset_path="analysis/synthetic_campaigns/${CAMPAIGN_NAME}/datasets/${dataset_name}.csv"
out_dir="analysis/full_matrix/${CAMPAIGN_NAME}_validation/${dataset_name}"

job_id=$(
  /usr/bin/sbatch --parsable \
    --job-name="ncdt_validate_${dataset_name}" \
    --time="$WALLTIME" \
    --export=ALL,PROJECT_ROOT="$PROJECT_ROOT",CONFIG="$CONFIG",MPI_LAUNCHER="$MPI_LAUNCHER",REPEAT="$REPEAT",PROCESS_COUNTS="$PROCESS_COUNTS",MODES="$MODES",DATASET_PATH="$dataset_path",OUT_DIR="$out_dir" \
    benchmarks/slurm_benchmark_dataset_matrix.sh | tail -n 1
)

echo "Submitted validation job ${job_id} for ${dataset_name}"
