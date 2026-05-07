# Configuration Guide

This directory stores JSON configuration files used by the CLI.

## Main File

- `default_config.json`: default schema, cleaning, characterization, cache, and
  benchmark settings for the project.

## Important Sections

- `schema`: manual overrides and excluded columns.
- `cleaning`: spike-detection, repair, and interpolation settings.
- `characterization`: dense interpolation or spline-characterization settings.
- `steady_state`: rules for deciding when a cleaned signal looks approximately steady versus clearly changing.
- `cache`: cache data type.
- `execution`: core serial/replicated/partitioned workflow defaults.
- `execution.default_modes`: default cleaning modes for workflow and scaling runs.
- `execution.analysis_nproc`: default MPI rank count for one-off replicated or partitioned analysis runs when `--analysis-nproc` is not provided.
- `execution.mpi_launcher`: default MPI launcher for workflow analysis runs.
- `benchmark`: optional performance-study defaults such as whether workflow benchmarking is enabled, repeat count, and MPI process counts.
- `benchmark.enabled_by_default`: whether `workflow` automatically runs repeated benchmark timing studies.

## Advice For New Users

Start with the default configuration unchanged. Only override values after you
have run inspection and understand the input file well enough to justify a
change.

## Central configuration idea

The repo is designed so the high-level `workflow` command reads one config file
and then dispatches to the lower-level modules. That means most users can
change behavior from one place instead of editing scripts:

- change `cleaning.window_radius`, `z_threshold`, or `strategy` to alter outlier repair,
- change `characterization.method` and `dense_factor` to alter spline-style output,
- change `steady_state.steady_window_seconds` when your system should be considered steady over a longer or shorter time window,
- change `steady_state.change_fraction_of_scale` and `std_fraction_of_scale` when the default steady-state summary is too strict or too loose,
- change `steady_state.groups` when certain sensors should be treated as one subsystem and evaluated together for group/system steady-state,
- change `execution.default_modes` if you want the repo to default to only certain cleaning modes.
- change `execution.analysis_nproc` if you want one fixed MPI rank count for the non-benchmark analysis runs inside `workflow`.
- change `execution.mpi_launcher` if your cluster uses something other than `mpirun`.
- change `benchmark.process_counts` and `benchmark.repeat` to alter optional MPI studies.
- change `benchmark.enabled_by_default` if you want workflow benchmarking to be opt-in or opt-out by default.

This keeps the orchestration centralized while still allowing the underlying
modules to be replaced independently.

For quick experimentation, users do not have to edit the config file just to
change the steady-state window once. The main commands also accept:

```bash
--steady-window-seconds <value>
```

That convenience flag overrides both the steady-state window length and the
minimum required steady duration for that run only.

## Benchmark Defaults

The default repo posture is now analysis-first. The `workflow` command does not
run repeated benchmark timing unless either:

- `benchmark.enabled_by_default` is set to `true` in the config, or
- you pass `--benchmark` on the CLI.

The flags:

```bash
--benchmark
--skip-benchmark
```

provide explicit CLI control when you want to override the config for one run.

Default workflow execution settings now come from:

```json
"execution": {
  "default_modes": ["serial", "replicated", "partitioned"],
  "analysis_nproc": null,
  "mpi_launcher": "mpirun"
}
```

If `analysis_nproc` is `null`, the workflow picks the largest configured
`process_count` greater than 1 for one-off replicated/partitioned analysis
runs.

Optional repeated timing-study settings remain under:

```json
"benchmark": {
  "enabled_by_default": false,
  "repeat": 3,
  "process_counts": [1, 2, 4, 8]
}
```

## MPI Mode Notes

- Use `serial` when you only have one rank or want the simplest correctness baseline.
- Use `replicated` when you have a small number of MPI ranks and enough memory for every rank to hold the full dataset.
- Use `partitioned` when you have more MPI ranks or larger row counts and want the more scalable time-domain decomposition.
- Practical rule of thumb: start with `serial`, move to `replicated` for a few ranks, then prefer `partitioned` once rank count or dataset size makes full-data duplication unattractive.

## Grouped steady-state analysis

The default config includes:

```json
"groups": {
  "all_sensors": ["*"]
}
```

That means the workflow will also report whether all tracked sensors are steady
at the same time.

You can define your own named groups when only certain sensors should count as
one subsystem. For example:

```json
"groups": {
  "all_sensors": ["*"],
  "thermal_loop": ["Sensor_A", "Sensor_B", "Sensor_C"],
  "pressure_pair": ["Inlet_Pressure", "Outlet_Pressure"]
}
```

Each named group will appear in `steady_state_groups_summary.csv` and in the
combined `steady_state_report.md`.
