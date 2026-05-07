# Source Tree Guide

This directory contains the installable Python package for the project.

## Layout

- `ncdt_cleaner/`: the actual package code.

## Where New Users Should Start

1. Read the top-level `README.md` for the workflow overview.
2. Read `ncdt_cleaner/README.md` for a module-by-module map.
3. Open `ncdt_cleaner/cli.py` if you want to understand command flow.
4. Open `ncdt_cleaner/workflow.py` if you want the high-level analysis workflow.

## Practical Note

Most users should not call internal functions directly at first. The safest
entrypoint is:

```bash
ncdt-cleaner workflow ...
```
