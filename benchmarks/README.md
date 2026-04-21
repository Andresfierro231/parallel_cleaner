# Benchmarks Guide

This directory contains helper scripts and locally generated benchmark inputs.

## Main Files

- `run_scaling.py`: standalone benchmark runner for an existing cache.
- `example_mpirun_commands.sh`: short example of the preferred workflow usage.
- `scaling_job_change_cache_dir.sh`: batch-job template for cluster execution.

## Recommended Usage

Most users should prefer the main workflow command:

```bash
python -m ncdt_cleaner.cli workflow /path/to/input.csv
```

Use `run_scaling.py` only when:

- you already have a cache,
- you want timings without rebuilding data products,
- you want to control benchmark output directory placement directly.

## Local Notes

- This directory may also contain scratch benchmark artifacts such as synthetic
  CSV files or per-session outputs used during development.
- Those generated artifacts are useful for experimentation but are not the core
  source of truth for the package itself.
