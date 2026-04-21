# Tests Guide

This project currently uses lightweight smoke-style validation rather than a
full unit-test suite.

## Main File

- `run_smoke_tests.py`: generates a small synthetic dataset and checks that the
  CLI still executes the major stages in sequence.

## Philosophy

The smoke test is intended to catch obvious breakage in:

- command parsing,
- synthetic data generation,
- inspection,
- cache building,
- the consolidated workflow command.

It is not intended to prove algorithmic correctness for every edge case.
