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
- `benchmark`: repeat count and MPI process counts.

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
- change `benchmark.process_counts` and `benchmark.repeat` to alter default MPI studies.

This keeps the orchestration centralized while still allowing the underlying
modules to be replaced independently.

For quick experimentation, users do not have to edit the config file just to
change the steady-state window once. The main commands also accept:

```bash
--steady-window-seconds <value>
```

That convenience flag overrides both the steady-state window length and the
minimum required steady duration for that run only.

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
