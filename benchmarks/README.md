# Benchmarks Guide

This directory is an advanced surface for performance studies. It is not part
of the recommended first-run path for newcomers.

## Start elsewhere first

Most users should start with the installed CLI and confirm the data-analysis
path works in serial mode:

```bash
ncdt-cleaner workflow /path/to/input.csv --characterize
```

Only move into `benchmarks/` after you already trust the cleaning outputs and
want to answer performance questions such as:

- how runtime changes with MPI rank count,
- whether `replicated` or `partitioned` is the better mode on available hardware,
- where MPI communication overhead begins to dominate.

## Main Files

- `run_scaling.py`: strong-scaling runner for a raw input file or an existing cache.
- `run_weak_scaling.py`: synthetic weak-scaling study for MPI-focused analysis.
- `make_synthetic_campaign.py`: generate a family of synthetic benchmark inputs plus a manifest.
- `example_mpirun_commands.sh`: short example of MPI workflow usage.
- `scaling_job_change_cache_dir.sh`: batch-job template for cluster execution.
- `slurm_strong_scaling_diagnostics.sh`: detailed strong-scaling run with timing-breakdown outputs.
- `slurm_weak_scaling_analysis.sh`: weak-scaling batch job helper.
- `submit_parallel_analysis_jobs.sh`: convenience helper for a full MPI study sequence.

## Which tool to use

Use `run_scaling.py` when:

- you want repeated runtime measurements from an existing cache or raw input,
- you want summary tables for elapsed time, speedup, and efficiency,
- you want per-repeat stdout and stderr logs collected under `logs/`.

Use `run_weak_scaling.py` when:

- you want to hold rows per rank roughly constant while increasing MPI ranks,
- you want to compare `replicated` and `partitioned` under a weak-scaling setup,
- you want synthetic datasets sized for an MPI study instead of real experimental data.

Use `make_synthetic_campaign.py` when:

- your real input is too small for meaningful scaling measurements,
- you want reproducible synthetic datasets with the same broad noise profile,
- you want a campaign manifest that can feed later scaling runs.

## Outputs from `run_scaling.py`

A strong-scaling run writes:

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

## Outputs from `run_weak_scaling.py`

A weak-scaling run writes:

- `weak_scaling_manifest.json`
- `weak_scaling_results.json`
- `weak_scaling_summary.json`
- `weak_scaling_summary.csv`
- `plots/`
- `logs/`

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

## Practical study sequence

1. Run `ncdt-cleaner workflow ... --characterize` and verify the cleaned outputs first.
2. Use `run_scaling.py` for strong-scaling timing on serial, replicated, and partitioned modes.
3. Use `run_weak_scaling.py` only if you need a deeper MPI scaling picture.
4. Use the timing-breakdown summaries to explain why one mode wins, not just that it wins.

## Local Notes

- This directory may also accumulate scratch synthetic datasets or benchmark outputs during development.
- Those generated artifacts are useful for experimentation, but they are secondary to the main cleaning workflow.
