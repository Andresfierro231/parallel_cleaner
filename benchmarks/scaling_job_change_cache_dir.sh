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

# FIXME: load your site modules here.
# module load python3/3.11
# module load openmpi

source .venv/bin/activate

INPUT_PATH="${1:-analysis/synth_2e6_16s.csv}"

python -m ncdt_cleaner.cli workflow "$INPUT_PATH" \
  --modes serial replicated partitioned \
  --process-counts 2 4 8
