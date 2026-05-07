# Reports Guide

This directory is a secondary documentation and figure-generation surface. It
exists for paper-style writeups and internal summaries, not for the main
newcomer workflow.

## Main Files

- `paper.tex`: the main LaTeX writeup.
- `references.bib`: bibliography entries.
- `paper_scaling_best_table.tex`: aggregated strong-scaling table.
- `make_parallel_scaling_figures.py`: combined strong-scaling figure generator.
- `make_mpi_diagnostics_figures.py`: diagnostics figure generator for weak scaling and timing breakdowns.

## Generated Files

You may also see generated LaTeX artifacts such as:

- `paper.pdf`
- `paper.aux`
- `paper.log`
- `_render_check/`

Those are build outputs rather than hand-maintained source documents.

## How it connects to the code

The main workflow and the optional benchmark runners emit JSON summaries, CSV
tables, and plots that can be reused here for technical writeups or reviews.

## Recommended sequence

1. Start with the main cleaning workflow:

```bash
ncdt-cleaner workflow /path/to/noisy_input.csv --characterize
```

2. If you need performance material, run the strong-scaling or weak-scaling tools from `benchmarks/`.
3. Regenerate combined figures:

```bash
.venv/bin/python reports/make_parallel_scaling_figures.py
.venv/bin/python reports/make_mpi_diagnostics_figures.py
```

4. Build `paper.tex` only after the figures and tables are in place:

```bash
cd reports
module load texlive/2023
pdflatex paper.tex
bibtex paper
pdflatex paper.tex
pdflatex paper.tex
```
