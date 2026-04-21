# Configuration Guide

This directory stores JSON configuration files used by the CLI.

## Main File

- `default_config.json`: default schema, cleaning, characterization, cache, and
  benchmark settings for the project.

## Important Sections

- `schema`: manual overrides and excluded columns.
- `cleaning`: spike-detection and repair settings.
- `characterization`: dense interpolation settings.
- `cache`: cache data type.
- `benchmark`: repeat count and MPI process counts.

## Advice For New Users

Start with the default configuration unchanged. Only override values after you
have run inspection and understand the input file well enough to justify a
change.
