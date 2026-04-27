#!/bin/bash
# File description:
# Generate the synthetic dataset family used by the paper scaling study.

#SBATCH -J ncdt-paper-campaign
#SBATCH -A ASC23046
#SBATCH -p NuclearEnergy
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -t 00:30:00
#SBATCH -o analysis/slurm-ncdt-paper-campaign-%j.out
#SBATCH -e analysis/slurm-ncdt-paper-campaign-%j.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-configs/benchmark_campaigns/default_campaign.json}"
CAMPAIGN_NAME="${CAMPAIGN_NAME:-paper_scaling_100k_1m}"
NUM_SENSORS="${NUM_SENSORS:-8}"
ROW_COUNTS="${ROW_COUNTS:-100000 200000 500000 1000000}"

read -r -a ROW_COUNTS_ARR <<< "$ROW_COUNTS"

cd "$PROJECT_ROOT"
source .venv/bin/activate

echo "[$(date --iso-8601=seconds)] Generating campaign ${CAMPAIGN_NAME}"
python benchmarks/make_synthetic_campaign.py \
  --config "$CAMPAIGN_CONFIG" \
  --campaign-name "$CAMPAIGN_NAME" \
  --rows "${ROW_COUNTS_ARR[@]}" \
  --num-sensors "$NUM_SENSORS"

echo "[$(date --iso-8601=seconds)] Campaign ready at analysis/synthetic_campaigns/${CAMPAIGN_NAME}"
