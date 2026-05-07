# Package Guide

This package implements an analysis-first sensor cleaning workflow with
optional parallel execution.

If you are new to the repo, start in the top-level `README.md` and run:

```bash
ncdt-cleaner workflow examples/tiny_sensor_example.csv --characterize
```

That is the quickest way to see cleaned outputs, spline-style characterization,
steady-state summaries, and the default signal overlays before reading the
internals below.

## Recommended Reading Order

1. `cli.py`
2. `workflow.py`
3. `inspectors.py`
4. `normalization.py`
5. `cache.py`
6. `cleaning.py`
7. `mpi_modes.py`
8. `benchmarks.py`

## Module Map

- `cli.py`: command-line entrypoint and session dispatch.
- `workflow.py`: centralized orchestration from raw noisy data to cleaned, characterized, and optionally benchmarked outputs.
- `inspectors.py`: safe input preview and schema guess summary.
- `readers.py`: format-specific file loading.
- `schema.py`: time-column and sensor-column inference.
- `normalization.py`: conversion from raw tables to normalized arrays.
- `cache.py`: cache write and load helpers.
- `cleaning.py`: local-window spike detection, repair, and interpolation.
- `characterize.py`: dense post-cleaning signal characterization, including cubic-spline fitting when available.
- `behavior.py`: practical steady-state versus changing summaries built from cleaned signals.
- `mpi_modes.py`: replicated and partitioned MPI cleaning modes.
- `benchmarks.py`: optional timing-study helpers, timing-breakdown aggregation, and benchmark summaries.
- `plotting.py`: plot generation for signal overlays and benchmark figures.
- `session.py`: dated output folders and logging.
- `stats.py`: small summary and CSV helpers.
- `synthetic.py`: synthetic dataset generation.
- `_mpi.py`: MPI initialization and shutdown safeguards.
- `xlsx_xml.py`: lightweight XLSX inspection helpers.
- `utils.py`: shared general helpers.
- `models.py`: small dataclasses shared across stages.

## For New Contributors

- Most pipeline stages return plain dictionaries plus explicit output paths.
- Session folders under `analysis/` are part of the user-facing contract.
- MPI modes should keep rank 0 as the single writer for shared output files.
- The clean/characterize sequence should remain the primary `workflow.py` path so users keep one high-level entrypoint.
- The new-user contract now also includes a short `steady_state_report.md` for characterized runs, so behavior summaries should stay easy to find and easy to read.
- Parallel cleaning support is a core product feature; benchmark tooling is a secondary performance-study surface built on top of it.
