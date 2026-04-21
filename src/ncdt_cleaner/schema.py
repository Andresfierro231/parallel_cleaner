'''
File description:
Schema inference helpers for identifying the time column and sensor columns.

This module contains the heuristics that make the project more tolerant of
messy engineering data with inconsistent headers and partially ambiguous fields.
'''

from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd

from .models import SchemaMapping, TimeColumnInference
from .utils import normalize_header

LOGGER = logging.getLogger(__name__)

TIME_NAME_WEIGHTS = {
    "time": 10.0,
    "t": 7.0,
    "seconds": 9.0,
    "second": 8.0,
    "sec": 8.0,
    "s": 5.0,
    "timestamp": 10.0,
    "datetime": 10.0,
    "date_time": 10.0,
    "elapsed_time": 10.0,
}


def _score_name(normalized: str) -> float:
    """Assign a heuristic score based on the normalized header text."""
    score = 0.0
    if normalized in TIME_NAME_WEIGHTS:
        score += TIME_NAME_WEIGHTS[normalized]
    if "time" in normalized:
        score += 5.0
    if "sec" in normalized:
        score += 3.0
    if "stamp" in normalized or "date" in normalized:
        score += 2.0
    return score


def _score_values(series: pd.Series) -> tuple[float, list[str]]:
    """Score a candidate time column by inspecting sampled values."""
    notes: list[str] = []
    score = 0.0
    trimmed = series.dropna().head(2000)
    if trimmed.empty:
        notes.append("all values are missing in sampled rows")
        return score, notes

    as_num = pd.to_numeric(trimmed, errors="coerce")
    numeric_fraction = float(as_num.notna().mean())
    if numeric_fraction > 0.9:
        score += 3.0
        diffs = np.diff(as_num.dropna().to_numpy())
        if diffs.size > 0 and np.all(diffs >= -1e-12):
            score += 3.0
            notes.append("values are monotone nondecreasing")
        if diffs.size > 0 and np.median(np.abs(diffs)) > 0:
            score += 1.5
            notes.append("values look like elapsed time")
    if numeric_fraction < 0.5:
        parsed_dt = pd.to_datetime(trimmed, errors="coerce")
        dt_fraction = float(parsed_dt.notna().mean())
        if dt_fraction > 0.9:
            score += 5.0
            notes.append("values parse as datetimes")
    return score, notes


def infer_time_column(
    df: pd.DataFrame,
    manual_time_column: str | None = None,
) -> TimeColumnInference:
    """Infer the most likely time column in a heterogeneous table."""
    original_headers = list(map(str, df.columns.tolist()))
    normalized_headers = [normalize_header(c) for c in original_headers]
    if manual_time_column is not None:
        if manual_time_column in original_headers:
            idx = original_headers.index(manual_time_column)
        else:
            idx = normalized_headers.index(normalize_header(manual_time_column))
        return TimeColumnInference(
            original_name=original_headers[idx],
            normalized_name=normalized_headers[idx],
            confidence=1.0,
            candidate_scores={original_headers[idx]: 1.0},
            reasoning=["manual override"],
            ambiguous=False,
        )

    scores: dict[str, float] = {}
    reasons: dict[str, list[str]] = {}
    for original, normalized in zip(original_headers, normalized_headers):
        # Combine lightweight name heuristics with observed value behavior so
        # columns can still score well even when headers are inconsistent.
        name_score = _score_name(normalized)
        value_score, notes = _score_values(df[original])
        score = name_score + value_score
        scores[original] = score
        reasons[original] = [f"name_score={name_score:.2f}", f"value_score={value_score:.2f}", *notes]

    if not scores:
        return TimeColumnInference(None, None, 0.0, {}, ["no columns found"], True)

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best_name, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else -np.inf
    best_norm = normalize_header(best_name)
    ambiguous = (best_score - second_score) < 1.5 or best_score < 4.0

    return TimeColumnInference(
        original_name=best_name,
        normalized_name=best_norm,
        confidence=float(min(best_score / 15.0, 1.0)),
        candidate_scores=scores,
        reasoning=reasons[best_name],
        ambiguous=ambiguous,
    )


def infer_sensor_columns(
    df: pd.DataFrame,
    time_column: str | None,
    manual_sensor_columns: list[str] | None = None,
    exclude_columns: Iterable[str] | None = None,
) -> list[str]:
    """Infer which columns should be treated as numeric sensor channels."""
    exclude = {normalize_header(c) for c in (exclude_columns or [])}
    if time_column is not None:
        exclude.add(normalize_header(time_column))
    if manual_sensor_columns:
        return [c for c in manual_sensor_columns if c in df.columns]

    sensors: list[str] = []
    for col in df.columns:
        norm = normalize_header(col)
        if norm in exclude:
            continue
        converted = pd.to_numeric(df[col], errors="coerce")
        numeric_fraction = float(converted.notna().mean()) if len(converted) else 0.0
        if numeric_fraction < 0.75:
            continue
        sensors.append(str(col))
    return sensors


def infer_schema(
    df: pd.DataFrame,
    manual_time_column: str | None = None,
    manual_sensor_columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
) -> SchemaMapping:
    """Infer the complete schema mapping used by normalization."""
    time_col = infer_time_column(df, manual_time_column=manual_time_column)
    sensors = infer_sensor_columns(
        df,
        time_column=time_col.original_name,
        manual_sensor_columns=manual_sensor_columns,
        exclude_columns=exclude_columns,
    )
    original_headers = list(map(str, df.columns.tolist()))
    normalized_headers = [normalize_header(c) for c in original_headers]
    notes: list[str] = []
    if time_col.ambiguous:
        notes.append("time column inference is ambiguous; consider explicit override")
        LOGGER.warning("Ambiguous time-column inference for columns: %s", time_col.candidate_scores)
    return SchemaMapping(
        original_headers=original_headers,
        normalized_headers=normalized_headers,
        time_column=time_col,
        sensor_columns=sensors,
        sensor_columns_normalized=[normalize_header(c) for c in sensors],
        excluded_columns=list(exclude_columns or []),
        notes=notes,
    )
