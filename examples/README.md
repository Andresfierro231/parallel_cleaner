# Example Data

This directory contains tiny tracked inputs that are safe to keep in Git and
use for a first run.

## Recommended first example

- `tiny_sensor_example.csv`

This file is intentionally small enough to run in a few seconds, but it still
contains:

- a time column in seconds,
- multiple sensor channels,
- a few spike-like outliers for the cleaning stage to repair,
- signals that enter, leave, and re-enter approximate steady-state.

## Suggested first command

From the repo root:

```bash
python -m ncdt_cleaner.cli workflow examples/tiny_sensor_example.csv \
  --characterize \
  --skip-benchmark
```

After the run, open:

- `steady_state_report.md`
- `steady_state_summary.csv`
- `steady_state_groups_summary.csv`
- `characterization_summary.csv`
- `sensors/*_rate_of_change.png`

Those files give the quickest overview of what changed, what looked
steady, and what the spline/characterization stage produced.

## README asset

The top-level README includes a tracked screenshot-style rendering of the
steady-state report:

- `steady_state_report_example.png`

If you ever want to regenerate that small asset, run:

```bash
python examples/make_example_assets.py
```
