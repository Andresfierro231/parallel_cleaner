'''
File description:
LabVIEW Measurement (LVM) text parsing helpers.

The VCU sample data includes common LabVIEW exports in two layouts:

- a single tabular block with an explicit `X_Value` time column,
- repeated segment blocks that omit explicit X values and instead require
  reconstructing time from `X0` and `Delta_X`.

This module parses both layouts into one tidy `pandas.DataFrame` so the rest of
the pipeline can treat LVM inputs the same way it treats CSV and XLSX data.
'''

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

SEPARATOR_MAP = {
    "tab": "\t",
    "comma": ",",
    "semicolon": ";",
}


def _split_tab_fields(line: str) -> list[str]:
    """Split one raw LVM line using the tab-oriented format used in samples."""
    return line.rstrip("\r\n").split("\t")


def _first_value(values: list[str] | None, default: str | None = None) -> str | None:
    """Return the first non-empty value from a parsed header field list."""
    if not values:
        return default
    for value in values:
        text = value.strip()
        if text:
            return text
    return default


def _parse_header(lines: list[str], start: int) -> tuple[dict[str, list[str]], int]:
    """Parse one LabVIEW header block ending at `***End_of_Header***`."""
    metadata: dict[str, list[str]] = {}
    idx = start
    while idx < len(lines):
        fields = _split_tab_fields(lines[idx])
        key = fields[0].strip().lstrip("\ufeff")
        if key == "***End_of_Header***":
            return metadata, idx + 1
        if key:
            metadata[key] = [field.strip() for field in fields[1:]]
        idx += 1
    raise ValueError("LVM header did not contain ***End_of_Header***")


def _next_nonempty_line(lines: list[str], start: int) -> int:
    """Advance past blank spacer lines between headers and data blocks."""
    idx = start
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    return idx


def _looks_like_segment_start(line: str) -> bool:
    """Detect the start of the next repeated LabVIEW data segment."""
    fields = _split_tab_fields(line)
    return bool(fields and fields[0].strip() == "Channels")


def _normalize_cell(value: str | float | int | None, decimal_separator: str) -> Any:
    """Normalize one parsed cell while preserving comments and missing values."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = value.strip()
    if not text:
        return None
    if decimal_separator != ".":
        text = text.replace(decimal_separator, ".")
    return text


def _fit_row_width(fields: list[str], ncols: int, delimiter: str) -> list[str]:
    """Pad or fold a row so it aligns with the parsed column header width."""
    if len(fields) < ncols:
        return fields + [""] * (ncols - len(fields))
    if len(fields) > ncols:
        return fields[: ncols - 1] + [delimiter.join(fields[ncols - 1 :])]
    return fields


def _parse_float(value: str | None, label: str) -> float:
    """Parse a required floating-point metadata field from an LVM header."""
    if value is None:
        raise ValueError(f"LVM segment is missing required metadata field: {label}")
    return float(value)


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Promote purely numeric columns to numeric dtypes for downstream logic."""
    for col in df.columns:
        series = df[col]
        converted = pd.to_numeric(series, errors="coerce")
        if int(converted.notna().sum()) == int(series.notna().sum()):
            df[col] = converted
    return df


def _parse_segment_frame(
    lines: list[str],
    start: int,
    *,
    decimal_separator: str,
    delimiter: str,
    default_x_columns: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], int]:
    """Parse one repeated LabVIEW data segment into a DataFrame."""
    segment_meta, idx = _parse_header(lines, start)
    idx = _next_nonempty_line(lines, idx)
    if idx >= len(lines):
        raise ValueError("LVM segment ended before the tabular data header row")

    columns = [field.strip() for field in _split_tab_fields(lines[idx])]
    idx += 1
    idx = _next_nonempty_line(lines, idx)

    raw_rows: list[list[Any]] = []
    while idx < len(lines):
        line = lines[idx]
        if not line.strip():
            peek = _next_nonempty_line(lines, idx + 1)
            if peek < len(lines) and _looks_like_segment_start(lines[peek]):
                idx = peek
                break
            idx += 1
            continue
        if _looks_like_segment_start(line):
            break
        fields = _fit_row_width(_split_tab_fields(line), len(columns), delimiter)
        raw_rows.append([_normalize_cell(value, decimal_separator) for value in fields])
        idx += 1

    frame = pd.DataFrame(raw_rows, columns=columns)
    x_columns = (_first_value(segment_meta.get("X_Columns"), default_x_columns) or "").lower()
    if x_columns == "no":
        x0 = _parse_float(_first_value(segment_meta.get("X0")), "X0")
        delta_x = _parse_float(_first_value(segment_meta.get("Delta_X")), "Delta_X")
        frame.iloc[:, 0] = [x0 + row_idx * delta_x for row_idx in range(len(frame))]

    frame = _coerce_numeric_columns(frame)
    return frame, segment_meta, idx


def read_lvm_frame(path: str | Path) -> pd.DataFrame:
    """Read a LabVIEW Measurement text file into one concatenated DataFrame."""
    path = Path(path)
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        lines = f.readlines()

    global_header, idx = _parse_header(lines, 0)
    idx = _next_nonempty_line(lines, idx)

    separator_name = (_first_value(global_header.get("Separator"), "Tab") or "Tab").lower()
    decimal_separator = _first_value(global_header.get("Decimal_Separator"), ".") or "."
    delimiter = SEPARATOR_MAP.get(separator_name, "\t")

    frames: list[pd.DataFrame] = []
    segment_headers: list[dict[str, Any]] = []
    while idx < len(lines):
        if not lines[idx].strip():
            idx += 1
            continue
        frame, segment_meta, idx = _parse_segment_frame(
            lines,
            idx,
            decimal_separator=decimal_separator,
            delimiter=delimiter,
            default_x_columns=_first_value(global_header.get("X_Columns")),
        )
        frames.append(frame)
        segment_headers.append(
            {
                "channels": _first_value(segment_meta.get("Channels")),
                "samples": _first_value(segment_meta.get("Samples")),
                "date": _first_value(segment_meta.get("Date")),
                "time": _first_value(segment_meta.get("Time")),
                "x_columns": _first_value(segment_meta.get("X_Columns"), _first_value(global_header.get("X_Columns"))),
                "x0": _first_value(segment_meta.get("X0")),
                "delta_x": _first_value(segment_meta.get("Delta_X")),
            }
        )

    if not frames:
        raise ValueError(f"No tabular data segments were found in LVM file: {path}")

    df = pd.concat(frames, ignore_index=True)
    df.attrs["lvm_metadata"] = {
        "global_header": {key: _first_value(value) for key, value in global_header.items()},
        "segment_count": len(frames),
        "segments": segment_headers,
    }
    return df
