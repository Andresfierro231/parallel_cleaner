#!/bin/bash
# File description:
# Submit the paper scaling-study jobs for 100k, 200k, 500k, and 1M datasets.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CAMPAIGN_NAME="${CAMPAIGN_NAME:-paper_scaling_100k_1m}"
CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-configs/benchmark_campaigns/default_campaign.json}"
CONFIG="${CONFIG:-configs/default_config.json}"
MPI_LAUNCHER="${MPI_LAUNCHER:-mpirun}"
REPEAT="${REPEAT:-3}"
NUM_SENSORS="${NUM_SENSORS:-8}"
ROW_COUNTS="${ROW_COUNTS:-100000 200000 500000 1000000}"
CAMPAIGN_JOB_ID="${CAMPAIGN_JOB_ID:-}"

cd "$PROJECT_ROOT"

if [[ -n "$CAMPAIGN_JOB_ID" ]]; then
  campaign_job_id="$CAMPAIGN_JOB_ID"
else
  campaign_job_id=$(
    /usr/bin/sbatch --parsable \
      --export=ALL,PROJECT_ROOT="$PROJECT_ROOT",CAMPAIGN_NAME="$CAMPAIGN_NAME",CAMPAIGN_CONFIG="$CAMPAIGN_CONFIG",NUM_SENSORS="$NUM_SENSORS",ROW_COUNTS="$ROW_COUNTS" \
      benchmarks/slurm_generate_campaign_matrix.sh | tail -n 1
  )
fi

echo "Submitted campaign job: ${campaign_job_id}"

submit_dataset_job() {
  local row_count="$1"
  local walltime="$2"
  local seed="$3"
  local dataset_name="${CAMPAIGN_NAME}_rows_${row_count}_sensors_${NUM_SENSORS}_seed_${seed}"
  local dataset_path="analysis/synthetic_campaigns/${CAMPAIGN_NAME}/datasets/${dataset_name}.csv"
  local out_dir="analysis/full_matrix/${CAMPAIGN_NAME}/${dataset_name}"
  local job_id

  job_id=$(
    /usr/bin/sbatch --parsable \
      --dependency="afterok:${campaign_job_id}" \
      --job-name="ncdt_${dataset_name}" \
      --time="$walltime" \
      --export=ALL,PROJECT_ROOT="$PROJECT_ROOT",CONFIG="$CONFIG",MPI_LAUNCHER="$MPI_LAUNCHER",REPEAT="$REPEAT",DATASET_PATH="$dataset_path",OUT_DIR="$out_dir" \
      benchmarks/slurm_benchmark_dataset_matrix.sh | tail -n 1
  )

  echo "Submitted ${dataset_name}: ${job_id}"
}

submit_dataset_job 100000 02:00:00 100
submit_dataset_job 200000 02:30:00 101
submit_dataset_job 500000 05:00:00 102
submit_dataset_job 1000000 09:00:00 103
