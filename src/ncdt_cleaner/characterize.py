from __future__ import annotations

import numpy as np

try:
    from scipy.interpolate import CubicSpline
    SCIPY_AVAILABLE = True
except Exception:  # pragma: no cover
    SCIPY_AVAILABLE = False
    CubicSpline = None


def characterize_signal(time: np.ndarray, values: np.ndarray, method: str = "cubic_spline", dense_factor: int = 4) -> dict:
    time = np.asarray(time, dtype=float)
    values = np.asarray(values, dtype=float)
    order = np.argsort(time)
    time = time[order]
    values = values[order]
    mask = ~np.isnan(values)
    time = time[mask]
    values = values[mask]
    unique_time, unique_idx = np.unique(time, return_index=True)
    time = unique_time
    values = values[unique_idx]
    if time.size < 4:
        return {
            "method": "insufficient_points",
            "dense_time": time.tolist(),
            "dense_values": values.tolist(),
        }

    dense_n = max(int(time.size * dense_factor), time.size)
    dense_time = np.linspace(float(time.min()), float(time.max()), dense_n)

    if method == "cubic_spline" and SCIPY_AVAILABLE:
        spline = CubicSpline(time, values, extrapolate=False)
        dense_values = spline(dense_time)
        return {
            "method": "cubic_spline",
            "dense_time": dense_time.tolist(),
            "dense_values": np.asarray(dense_values, dtype=float).tolist(),
        }

    # local cubic fallback
    dense_values = np.interp(dense_time, time, values)
    return {
        "method": "linear_interp_fallback",
        "dense_time": dense_time.tolist(),
        "dense_values": dense_values.tolist(),
    }
