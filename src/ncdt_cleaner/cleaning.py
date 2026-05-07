'''
File description:
Core spike detection and repair logic for one sensor time series.

This is the algorithmic center of the project. It flags suspicious points using
local-window statistics, then applies a configurable repair strategy and emits
summary statistics that the rest of the pipeline records.
'''

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

LOGGER = logging.getLogger(__name__)

try:
    from numba import njit
    NUMBA_AVAILABLE = True
except Exception:  # pragma: no cover
    NUMBA_AVAILABLE = False
    def njit(*args, **kwargs):
        def wrapper(fn):
            return fn
        return wrapper


@njit(cache=True)
def _flag_spikes_numba(values: np.ndarray, window_radius: int, z_threshold: float, absolute_jump_threshold: float) -> np.ndarray:
    """Numba-compatible implementation of local-window spike flagging."""
    n = len(values)
    flags = np.zeros(n, dtype=np.uint8)
    for i in range(n):
        left = max(0, i - window_radius)
        right = min(n, i + window_radius + 1)
        count = 0
        mean = 0.0
        for j in range(left, right):
            if j == i:
                continue
            v = values[j]
            if np.isnan(v):
                continue
            mean += v
            count += 1
        if count < 2 or np.isnan(values[i]):
            continue
        mean /= count
        var = 0.0
        for j in range(left, right):
            if j == i:
                continue
            v = values[j]
            if np.isnan(v):
                continue
            diff = v - mean
            var += diff * diff
        std = np.sqrt(var / max(count - 1, 1))
        deviation = abs(values[i] - mean)
        z_ok = std > 0 and deviation > z_threshold * std
        jump_ok = absolute_jump_threshold > 0 and deviation > absolute_jump_threshold
        if z_ok or jump_ok:
            flags[i] = 1
    return flags


def flag_spikes(values: np.ndarray, window_radius: int, z_threshold: float, absolute_jump_threshold: float) -> np.ndarray:
    """Public wrapper for spike flagging that normalizes input types."""
    arr = np.asarray(values, dtype=float)
    if NUMBA_AVAILABLE:
        return _flag_spikes_numba(arr, int(window_radius), float(z_threshold), float(absolute_jump_threshold)).astype(bool)
    return _flag_spikes_numba(arr, int(window_radius), float(z_threshold), float(absolute_jump_threshold)).astype(bool)


def _nearest_valid(values: np.ndarray, i: int) -> float:
    """Return the closest non-NaN value near index `i`."""
    n = len(values)
    for offset in range(1, n):
        left = i - offset
        right = i + offset
        if left >= 0 and not np.isnan(values[left]):
            return values[left]
        if right < n and not np.isnan(values[right]):
            return values[right]
    return np.nan


def _local_mean(values: np.ndarray, i: int, window_radius: int) -> float:
    """Compute the neighborhood mean around one index, excluding itself."""
    left = max(0, i - window_radius)
    right = min(len(values), i + window_radius + 1)
    window = np.delete(values[left:right], min(i - left, right - left - 1))
    window = window[~np.isnan(window)]
    return float(window.mean()) if window.size else np.nan


def apply_repair_strategy(
    values: np.ndarray,
    flags: np.ndarray,
    strategy: str,
    window_radius: int,
    clip_max: float | None = None,
) -> np.ndarray:
    """Repair flagged points according to the configured strategy name."""
    repaired = np.asarray(values, dtype=float).copy()
    for i, flagged in enumerate(flags):
        if not flagged:
            continue
        if strategy == "drop":
            repaired[i] = np.nan
        elif strategy == "nearest":
            repaired[i] = _nearest_valid(repaired, i)
        elif strategy == "local_mean":
            repaired[i] = _local_mean(repaired, i, window_radius)
        elif strategy == "clip":
            base = _local_mean(repaired, i, window_radius)
            if clip_max is None:
                repaired[i] = base
            else:
                repaired[i] = np.clip(base, -abs(clip_max), abs(clip_max))
        else:
            raise ValueError(f"Unknown repair strategy: {strategy}")
    return repaired


def fill_missing(values: np.ndarray) -> np.ndarray:
    """Fill missing values by interpolation or constant fallback behavior."""
    arr = np.asarray(values, dtype=float).copy()
    idx = np.arange(arr.size)
    mask = ~np.isnan(arr)
    if mask.sum() == 0:
        return np.zeros_like(arr)
    if mask.sum() == 1:
        arr[~mask] = arr[mask][0]
        return arr
    arr[~mask] = np.interp(idx[~mask], idx[mask], arr[mask])
    return arr


@dataclass
class SensorCleaningResult:
    """Bundle original values, cleaned values, flags, and summary stats."""
    original: np.ndarray
    cleaned: np.ndarray
    flags: np.ndarray
    stats: dict


def clean_sensor(values: np.ndarray, cleaning_cfg: dict) -> SensorCleaningResult:
    """Run the full cleaning pipeline for a single sensor channel."""
    original = np.asarray(values, dtype=float)
    flags = flag_spikes(
        original,
        window_radius=int(cleaning_cfg["window_radius"]),
        z_threshold=float(cleaning_cfg["z_threshold"]),
        absolute_jump_threshold=float(cleaning_cfg["absolute_jump_threshold"]),
    )
    cleaned = apply_repair_strategy(
        original,
        flags,
        strategy=cleaning_cfg["strategy"],
        window_radius=int(cleaning_cfg["window_radius"]),
        clip_max=cleaning_cfg.get("clip_max"),
    )
    if cleaning_cfg.get("nan_policy", "interpolate") == "interpolate":
        cleaned = fill_missing(cleaned)
    stats = {
        "n_points": int(original.size),
        "n_flagged": int(flags.sum()),
        "fraction_flagged": float(flags.mean()) if original.size else 0.0,
        "mean_original": float(np.nanmean(original)),
        "mean_cleaned": float(np.nanmean(cleaned)),
        "std_original": float(np.nanstd(original)),
        "std_cleaned": float(np.nanstd(cleaned)),
        "min_cleaned": float(np.nanmin(cleaned)),
        "max_cleaned": float(np.nanmax(cleaned)),
    }
    return SensorCleaningResult(original=original, cleaned=cleaned, flags=flags, stats=stats)
