#!/bin/bash
# File description:
# Submit the additional MPI-focused analyses used by the report.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"

cd "$PROJECT_ROOT"

echo "Submitting strong-scaling diagnostics job..."
/usr/bin/sbatch benchmarks/slurm_strong_scaling_diagnostics.sh

echo "Submitting weak-scaling job..."
/usr/bin/sbatch benchmarks/slurm_weak_scaling_analysis.sh
