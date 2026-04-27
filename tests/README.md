# Tests Guide

This project currently uses lightweight smoke-style validation rather than a
full unit-test suite.

## Main File

- `run_smoke_tests.py`: generates a small synthetic dataset and checks that the
  CLI still executes the major stages in sequence.
- `test_behavior_summary.py`: focused check that the steady-state summary can
  distinguish a flat region from a later breakout/change region.

## Philosophy

The smoke test is intended to catch obvious breakage in:

- command parsing,
- synthetic data generation,
- inspection,
- cache building,
- the consolidated workflow command.
- the user-facing steady-state summary logic.

It is not intended to prove algorithmic correctness for every edge case.
