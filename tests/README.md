# Tests Guide

The repo now treats serial analysis as the baseline deployment path, so the
test suite is centered on that contract first and then layers focused coverage
around specific parsing and summary behavior.

## Main Files

- `test_cli_serial_integration.py`: end-to-end serial workflow checks, including default plot generation and `--skip-plots`.
- `test_lvm_support.py`: `.lvm` parsing, inspection, and normalization coverage.
- `test_behavior_summary.py`: focused steady-state and breakout summary checks.
- `test_synthetic_campaign_generator.py`: synthetic benchmark dataset generation checks.
- `test_packaging_metadata.py`: packaging checks for the console entry point and optional MPI dependency split.
- `run_smoke_tests.py`: optional local smoke helper that exercises the main CLI path plus the standalone scaling runner.

## Philosophy

The default safety net should catch obvious breakage in:

- serial CLI execution from raw input to cleaned outputs,
- generated overlay plots and their opt-out behavior,
- supported input parsing, including `.lvm`,
- steady-state summary behavior,
- and synthetic dataset generation used by advanced scaling studies.

MPI scaling behavior remains important, but the base test contract should still
pass on a machine that only installs the serial dependency set.
