#!/bin/bash
# File description:
# Batch-job template for launching the consolidated workflow command on a
# cluster allocation. Replace the module lines and input path as needed.
#
#SBATCH -J ncdt_workflow
#SBATCH -N 1
#SBATCH -n 8
#SBATCH -t 01:00:00
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

# FIXME: replace with the raw input file you want to benchmark.
INPUT_PATH="${1:-analysis/synth_2e6_16s.csv}"

# FIXME: tune process counts, repeat count, and launcher to match your node.
python benchmarks/run_scaling.py \
  --input-file "$INPUT_PATH" \
  --process-counts 2 4 8 \
  --repeat 1
