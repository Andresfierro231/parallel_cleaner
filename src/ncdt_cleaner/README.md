# Package Guide

This package implements the end-to-end sensor cleaning workflow used by the
project report.

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
- `workflow.py`: consolidated workflow command.
- `inspectors.py`: safe input preview and schema guess summary.
- `readers.py`: format-specific file loading.
- `schema.py`: time-column and sensor-column inference.
- `normalization.py`: conversion from raw tables to normalized arrays.
- `cache.py`: cache write and load helpers.
- `cleaning.py`: spike detection and repair.
- `characterize.py`: dense post-cleaning signal characterization.
- `mpi_modes.py`: replicated and partitioned MPI execution modes.
- `benchmarks.py`: timing-study helpers and benchmark summaries.
- `plotting.py`: plot generation for runtime and speedup figures.
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
