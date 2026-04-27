# Reports Guide

This directory contains the paper draft and its bibliography.

## Main Files

- `paper.tex`: the main LaTeX paper.
- `references.bib`: bibliography entries.
- `paper_scaling_best_table.tex`: aggregated strong-scaling table for the paper.
- `make_parallel_scaling_figures.py`: combined strong-scaling figure generator.
- `make_mpi_diagnostics_figures.py`: diagnostics figure generator for weak scaling and timing breakdowns.

## Generated Files

You may also see generated LaTeX artifacts such as:

- `paper.pdf`
- `paper.aux`
- `paper.log`
- `_render_check/`

Those are build outputs and visual checks rather than hand-maintained source
documents.

## How It Connects To The Code

The workflow command writes benchmark summaries, plots, and cleaning tables that
can be referenced directly from this paper directory when preparing final
results.

## Recommended report asset workflow

1. Run the centralized pipeline from raw data when you want cleaned outputs and optional spline characterization:

```bash
python -m ncdt_cleaner.cli workflow /path/to/noisy_input.csv --characterize
```

2. Run the strong-scaling and weak-scaling studies from `benchmarks/`.
3. Regenerate paper figures:

```bash
.venv/bin/python reports/make_parallel_scaling_figures.py
.venv/bin/python reports/make_mpi_diagnostics_figures.py
```

4. Build `paper.tex` after the figures and tables are in place:

```bash
cd reports
module load texlive/2023
pdflatex paper.tex
bibtex paper
pdflatex paper.tex
pdflatex paper.tex
```

The paper now cites the MPI standard, `mpi4py`, SciPy, and the core scaling-law
references used in the discussion.
