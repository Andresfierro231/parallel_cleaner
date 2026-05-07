#!/bin/bash
# File description:
# SLURM batch script for running the scaling benchmark matrix across four
# synthetic dataset sizes using serial plus MPI-backed replicated/partitioned
# modes. The job requests the maximum task count once, then benchmarks the
# lower task counts sequentially inside that allocation.
#
# Submit with:
#   /usr/bin/sbatch benchmarks/slurm_scaling_matrix_100k_1m.sh
#
# Optional overrides:
#   PROJECT_ROOT=/path/to/repo
#   CONFIG=configs/default_config.json
#   CAMPAIGN_CONFIG=configs/benchmark_campaigns/default_campaign.json
#   CAMPAIGN_NAME=scaling_100k_1m
#   REPEAT=1
#   MPI_LAUNCHER=srun
#   NUM_SENSORS=8

#SBATCH -J ncdt-scale-matrix
#SBATCH -n 256
#SBATCH -p NuclearEnergy
#SBATCH -A ASC23046
#SBATCH -t 100:00:00
#SBATCH -o analysis/slurm-ncdt-scale-matrix-%j.out
#SBATCH -e analysis/slurm-ncdt-scale-matrix-%j.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CONFIG="${CONFIG:-configs/default_config.json}"
CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-configs/benchmark_campaigns/default_campaign.json}"
CAMPAIGN_NAME="${CAMPAIGN_NAME:-scaling_100k_1m}"
MPI_LAUNCHER="${MPI_LAUNCHER:-srun}"
REPEAT="${REPEAT:-1}"
NUM_SENSORS="${NUM_SENSORS:-8}"

ROW_COUNTS=(100000 200000 500000 1000000)
PROCESS_COUNTS=(1 2 4 8 16 24 48 64 96 128)

cd "$PROJECT_ROOT"
source .venv/bin/activate

CAMPAIGN_ROOT="analysis/synthetic_campaigns/${CAMPAIGN_NAME}"
DATASET_DIR="${CAMPAIGN_ROOT}/datasets"
OUT_ROOT="analysis/full_matrix/${CAMPAIGN_NAME}"

mkdir -p "$OUT_ROOT"

echo "[$(date --iso-8601=seconds)] Generating benchmark campaign ${CAMPAIGN_NAME}"
python benchmarks/make_synthetic_campaign.py \
  --config "$CAMPAIGN_CONFIG" \
  --campaign-name "$CAMPAIGN_NAME" \
  --rows "${ROW_COUNTS[@]}" \
  --num-sensors "$NUM_SENSORS"

for row_count in "${ROW_COUNTS[@]}"; do
  input_path="$(printf '%s\n' "${DATASET_DIR}"/*"_rows_${row_count}_sensors_${NUM_SENSORS}_"*.csv | head -n 1)"
  if [[ ! -f "$input_path" ]]; then
    echo "Dataset for ${row_count} rows was not generated under ${DATASET_DIR}" >&2
    exit 1
  fi

  dataset_name="$(basename "${input_path%.csv}")"
  out_dir="${OUT_ROOT}/${dataset_name}"

  echo "[$(date --iso-8601=seconds)] Benchmarking ${dataset_name}"
  python benchmarks/run_scaling.py \
    --input-file "$input_path" \
    --config "$CONFIG" \
    --modes serial replicated partitioned \
    --process-counts "${PROCESS_COUNTS[@]}" \
    --repeat "$REPEAT" \
    --mpi-launcher "$MPI_LAUNCHER" \
    --out-dir "$out_dir"
done

echo "[$(date --iso-8601=seconds)] Completed scaling matrix under ${OUT_ROOT}"
