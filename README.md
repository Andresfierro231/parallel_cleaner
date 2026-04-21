# NCDT Parallel Sensor Cleaning and Characterization Project

This repository contains a modular Python + MPI codebase for:
- inspecting heterogeneous engineering data files,
- inferring schemas with irregular headers,
- normalizing sensor time-series into a common internal format,
- cleaning spikes/noise using configurable local-window methods,
- computing descriptive statistics,
- building local cubic / spline-style characterizations,
- comparing serial, replicated MPI, and partitioned MPI execution,
- logging all work into dated session folders under `analysis/`,
- preparing report artifacts for a scientific-style write-up.

The code is intentionally designed around two use cases:
1. **course project deliverables** on modest data, and
2. **future large-scale workflows** involving many GB/TB of CSV archives or streaming data.

## Repository layout

- `src/ncdt_cleaner/` - library code and CLI
- `configs/` - default configuration files
- `analysis/` - generated dated session folders
- `benchmarks/` - benchmark runners and helper scripts
- `reports/` - LaTeX paper draft
- `c_openmp/` - focused C/OpenMP comparison kernel
- `tests/` - smoke-test helpers

## Quick start
cd to ncdt_parallel_cleaner_project/

Create an environment with the dependencies in `pyproject.toml`
```bash
python3.11 -m venv .venv # Skip if you already have a .venv
source .venv/bin/activate
python --version
python -m pip install --upgrade pip
python -m pip install -e .
python -m pip install numpy pandas
```

Then:

Get on a compute notde, 

```bash
python -m pip install -e .
python -m ncdt_cleaner.cli workflow /path/to/Test00.xlsx \
  --modes serial replicated partitioned \
  --process-counts 2 4 8
```

## Intended workflow

1. **Inspect** raw files.
2. **Infer schema** and confirm or override it.
3. **Cache-build** a binary on-disk representation (`.npy`/memmap-friendly arrays + metadata).
4. **Clean** the data.
5. **Characterize** the cleaned data using splines or local cubics.
6. **Benchmark** serial vs MPI strategies.
7. Use the generated JSON/CSV/plots in the report draft under `reports/`.

The recommended entrypoint is now:

```bash
python -m ncdt_cleaner.cli workflow /path/to/Test00.xlsx
```

This single command can:
- inspect the input,
- create the cache if needed,
- run representative serial, replicated, and partitioned clean passes,
- benchmark the requested process counts,
- write a consolidated `workflow_report.json`,
- emit report-friendly files such as `benchmark_summary.csv` and benchmark plots.

If you already have a cache, reuse it directly:

```bash
python -m ncdt_cleaner.cli workflow --cache-dir analysis/<session>/cache
```

## Notes on file irregularities

The code never assumes:
- the time column is literally named `time`,
- all files have the same headers,
- all data are already tidy.

Instead it:
- normalizes headers,
- scores time-column candidates by name and value behavior,
- logs ambiguities,
- allows explicit CLI/config overrides.

## Large-data design

For future many-GB/TB workflows, the code separates:
- raw ingest,
- schema inference,
- normalized cache creation,
- analysis on binary arrays.

This avoids repeated expensive CSV parsing and enables memmap-backed partitioned processing.

## Report

The scientific-paper-style draft is in:

- `reports/paper.tex`
- `reports/references.bib`

## OpenMP comparison

The focused companion comparison is in:

- `c_openmp/spike_kernel.c`
- `c_openmp/Makefile`

It is intentionally a small kernel, not the full application.
