# Benchmarks Guide

This directory contains helper scripts and locally generated benchmark inputs.

## Main Files

- `run_scaling.py`: standalone benchmark runner for a raw input file or an existing cache.
- `run_weak_scaling.py`: synthetic weak-scaling study for MPI-focused analysis.
- `make_synthetic_campaign.py`: generate a family of synthetic benchmark inputs plus a manifest.
- `example_mpirun_commands.sh`: short example of the preferred workflow usage.
- `scaling_job_change_cache_dir.sh`: batch-job template for cluster execution.
- `slurm_strong_scaling_diagnostics.sh`: detailed strong-scaling run with timing-breakdown outputs.
- `slurm_weak_scaling_analysis.sh`: weak-scaling batch job for the report.
- `submit_parallel_analysis_jobs.sh`: convenience helper that submits both MPI-focused report analyses.

## Recommended Usage

Most users should prefer the main workflow command:

```bash
python -m ncdt_cleaner.cli workflow /path/to/input.csv
```

Use `run_scaling.py` when:

- you want one clean benchmark report JSON at the end,
- you want per-repeat stdout/stderr logs saved under `logs/`,
- you want a benchmark manifest describing the resolved input/cache/config,
- you want timings without manually copying cache paths,
- you want timing-breakdown and parallel-metrics summaries for MPI analysis.

Use `run_weak_scaling.py` when:

- you want to study whether runtime stays approximately constant as work per rank is held fixed,
- you want a stronger MPI discussion than strong scaling alone can provide,
- you want a synthetic campaign that scales rows proportionally with rank count.

Use `make_synthetic_campaign.py` when:

- your real input file is too small for meaningful scaling measurements,
- you want a reproducible family of datasets with the same noise/spike profile,
- you want one campaign manifest that can drive the rest of the benchmark study.

## Outputs from `run_scaling.py`

A benchmark run now writes:

- `benchmark_manifest.json`
- `benchmark_results.json`
- `benchmark_summary.json`
- `benchmark_summary.csv`
- `timing_breakdown_summary.json`
- `timing_breakdown_summary.csv`
- `parallel_metrics_summary.json`
- `parallel_metrics_summary.csv`
- `plots/`
- `logs/`

The `logs/` directory contains one stdout/stderr pair per repeated benchmark run,
which is more Slurm-friendly than mixing child JSON payloads into the parent
benchmark runner stdout stream.

## Outputs from `run_weak_scaling.py`

A weak-scaling run writes:

- `weak_scaling_manifest.json`
- `weak_scaling_results.json`
- `weak_scaling_summary.json`
- `weak_scaling_summary.csv`
- `plots/`
- `logs/`

The weak-scaling summary reports runtime and weak-scaling efficiency against the
smallest-rank baseline for each MPI mode.

## Outputs from `make_synthetic_campaign.py`

A synthetic campaign writes:

- `campaign_manifest.json`
- `campaign_summary.csv`
- `datasets/`

Example:

```bash
python benchmarks/make_synthetic_campaign.py \
  --campaign-name scaling_default \
  --rows 100000 500000 1000000 5000000 \
  --num-sensors 8 \
  --noise-sigma 0.35 \
  --spike-fraction 0.002
```

That produces a dataset family suitable for serial vs MPI scaling comparisons.

## Recommended study sequence for the report

1. Use `python -m ncdt_cleaner.cli workflow ... --characterize` to prove the raw-data-to-output pipeline.
2. Use `run_scaling.py` or the batch helpers for strong scaling.
3. Use `run_weak_scaling.py` or `slurm_weak_scaling_analysis.sh` for weak scaling.
4. Use the timing-breakdown summaries from the detailed strong-scaling run to explain MPI overheads, not just timings.

## Local Notes

- This directory may also contain scratch benchmark artifacts such as synthetic
  CSV files or per-session outputs used during development.
- Those generated artifacts are useful for experimentation but are not the core
  source of truth for the package itself.
