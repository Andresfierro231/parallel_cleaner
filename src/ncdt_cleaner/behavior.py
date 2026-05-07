"""
User-facing signal behavior summaries built from cleaned time-series data.

The goal of this module is not formal control-theory state estimation. It gives
new users a practical answer to a simpler question:

"Where does the signal look approximately steady for a sustained time window,
and where does it clearly start changing again?"
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from .stats import write_csv_table
from .utils import normalize_header


def _prefix_sum(values: np.ndarray) -> np.ndarray:
    return np.concatenate(([0.0], np.cumsum(values, dtype=float)))


def _window_sum(prefix: np.ndarray, left: np.ndarray, right: np.ndarray) -> np.ndarray:
    return prefix[right] - prefix[left]


def _format_time(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f} s"


def _default_behavior_cfg() -> dict:
    return {
        "steady_window_seconds": 60.0,
        "min_steady_duration_seconds": 60.0,
        "min_points_per_window": 5,
        "change_fraction_of_scale": 0.15,
        "std_fraction_of_scale": 0.10,
        "slope_tolerance_per_second": None,
        "max_change_in_window": None,
        "max_std_in_window": None,
        "groups": {
            "all_sensors": ["*"],
        },
    }


def analyze_signal_behavior(time: np.ndarray, values: np.ndarray, behavior_cfg: dict | None = None) -> tuple[dict, dict]:
    """Return a user-facing summary plus internal details for one cleaned signal."""
    cfg = {
        key: value for key, value in _default_behavior_cfg().items() if key != "groups"
    }
    if behavior_cfg:
        cfg.update({key: value for key, value in behavior_cfg.items() if key != "groups"})

    time = np.asarray(time, dtype=float)
    values = np.asarray(values, dtype=float)
    order = np.argsort(time)
    time = time[order]
    values = values[order]
    mask = ~(np.isnan(time) | np.isnan(values))
    time = time[mask]
    values = values[mask]
    if time.size < 3:
        summary_text = "Not enough valid points to estimate steady-state behavior."
        return {
            "method": "steady_state_window",
            "n_samples": int(time.size),
            "n_steady_segments": 0,
            "steady_segments": [],
            "state_transitions": [],
            "summary_text": summary_text,
            "thresholds": {},
        }, {"time": time.tolist(), "steady_mask": []}

    unique_time, unique_idx = np.unique(time, return_index=True)
    time = unique_time
    values = values[unique_idx]
    if time.size < 3:
        summary_text = "Not enough unique time samples to estimate steady-state behavior."
        return {
            "method": "steady_state_window",
            "n_samples": int(time.size),
            "n_steady_segments": 0,
            "steady_segments": [],
            "state_transitions": [],
            "summary_text": summary_text,
            "thresholds": {},
        }, {"time": time.tolist(), "steady_mask": []}

    window_seconds = float(cfg["steady_window_seconds"])
    min_duration = float(cfg.get("min_steady_duration_seconds") or window_seconds)
    min_points = max(int(cfg.get("min_points_per_window", 5)), 3)
    half_window = window_seconds / 2.0

    signal_scale = max(
        float(np.nanstd(values)),
        float((np.nanmax(values) - np.nanmin(values)) / 6.0) if values.size else 0.0,
        1e-12,
    )
    max_change = cfg.get("max_change_in_window")
    if max_change is None:
        max_change = float(cfg.get("change_fraction_of_scale", 0.15)) * signal_scale
    max_std = cfg.get("max_std_in_window")
    if max_std is None:
        max_std = float(cfg.get("std_fraction_of_scale", 0.10)) * signal_scale
    slope_tol = cfg.get("slope_tolerance_per_second")
    if slope_tol is None:
        slope_tol = max_change / max(window_seconds, 1e-12)

    left = np.searchsorted(time, time - half_window, side="left")
    right = np.searchsorted(time, time + half_window, side="right")
    count = right - left
    span = np.where(count > 1, time[np.clip(right - 1, 0, time.size - 1)] - time[left], 0.0)

    prefix_t = _prefix_sum(time)
    prefix_y = _prefix_sum(values)
    prefix_tt = _prefix_sum(time * time)
    prefix_ty = _prefix_sum(time * values)
    prefix_yy = _prefix_sum(values * values)

    n = count.astype(float)
    sum_t = _window_sum(prefix_t, left, right)
    sum_y = _window_sum(prefix_y, left, right)
    sum_tt = _window_sum(prefix_tt, left, right)
    sum_ty = _window_sum(prefix_ty, left, right)
    sum_yy = _window_sum(prefix_yy, left, right)

    denom = n * sum_tt - sum_t * sum_t
    slope = np.zeros_like(time, dtype=float)
    valid_denom = denom > 1e-12
    slope[valid_denom] = (n[valid_denom] * sum_ty[valid_denom] - sum_t[valid_denom] * sum_y[valid_denom]) / denom[valid_denom]

    mean_y = np.divide(sum_y, n, out=np.zeros_like(sum_y), where=n > 0)
    variance = np.divide(sum_yy, n, out=np.zeros_like(sum_yy), where=n > 0) - mean_y * mean_y
    variance = np.maximum(variance, 0.0)
    local_std = np.sqrt(variance)
    endpoint_change = np.where(count > 1, np.abs(values[np.clip(right - 1, 0, values.size - 1)] - values[left]), 0.0)

    steady_mask = (
        (count >= min_points)
        & (span >= min_duration)
        & (np.abs(slope) <= float(slope_tol))
        & (endpoint_change <= float(max_change))
        & (local_std <= float(max_std))
    )

    steady_segments, transitions = _segments_from_mask(
        time=time,
        steady_mask=steady_mask,
        build_segment=lambda start_idx, end_idx: _build_segment(time, values, start_idx, end_idx, slope, endpoint_change, local_std),
        min_duration=min_duration,
    )

    total_steady = float(sum(segment["duration_seconds"] for segment in steady_segments))
    longest = max(steady_segments, key=lambda segment: segment["duration_seconds"], default=None)
    first_breakout = transitions[0]["time"] if transitions else None
    if longest is None:
        summary_text = (
            f"No sustained steady-state window of at least {min_duration:.0f} s was detected. "
            "The signal appears to keep changing or stays too noisy for the current thresholds."
        )
    else:
        summary_text = (
            f"Detected {len(steady_segments)} steady-state window(s). "
            f"Longest steady window: {_format_time(longest['start_time'])} to {_format_time(longest['end_time'])} "
            f"({longest['duration_seconds']:.2f} s)."
        )
        if first_breakout is not None:
            summary_text += f" First clear breakout from steady-state occurs near {_format_time(first_breakout)}."

    summary = {
        "method": "steady_state_window",
        "n_samples": int(time.size),
        "signal_scale": float(signal_scale),
        "thresholds": {
            "steady_window_seconds": float(window_seconds),
            "min_steady_duration_seconds": float(min_duration),
            "min_points_per_window": int(min_points),
            "slope_tolerance_per_second": float(slope_tol),
            "max_change_in_window": float(max_change),
            "max_std_in_window": float(max_std),
        },
        "n_steady_segments": len(steady_segments),
        "total_steady_duration_seconds": total_steady,
        "longest_steady_segment": longest,
        "steady_segments": steady_segments,
        "state_transitions": transitions,
        "summary_text": summary_text,
    }
    detail = {
        "time": time.tolist(),
        "steady_mask": steady_mask.astype(bool).tolist(),
        "thresholds": summary["thresholds"],
    }
    return summary, detail


def summarize_signal_behavior(time: np.ndarray, values: np.ndarray, behavior_cfg: dict | None = None) -> dict:
    """Classify a cleaned signal into steady-state and changing intervals."""
    summary, _ = analyze_signal_behavior(time, values, behavior_cfg)
    return summary


def _segments_from_mask(
    *,
    time: np.ndarray,
    steady_mask: np.ndarray,
    build_segment,
    min_duration: float,
) -> tuple[list[dict], list[dict]]:
    steady_segments: list[dict] = []
    transitions: list[dict] = []
    if not np.any(steady_mask):
        return steady_segments, transitions

    start_idx = None
    for idx, is_steady in enumerate(steady_mask):
        if is_steady and start_idx is None:
            start_idx = idx
        elif not is_steady and start_idx is not None:
            segment = build_segment(start_idx, idx - 1)
            if segment["duration_seconds"] >= min_duration:
                steady_segments.append(segment)
                if idx < time.size:
                    transitions.append(
                        {
                            "time": float(time[idx]),
                            "from_state": "steady",
                            "to_state": "changing",
                            "after_segment_start": float(segment["start_time"]),
                            "after_segment_end": float(segment["end_time"]),
                        }
                    )
            start_idx = None
    if start_idx is not None:
        segment = build_segment(start_idx, time.size - 1)
        if segment["duration_seconds"] >= min_duration:
            steady_segments.append(segment)
    return steady_segments, transitions


def _build_segment(
    time: np.ndarray,
    values: np.ndarray,
    start_idx: int,
    end_idx: int,
    slope: np.ndarray,
    endpoint_change: np.ndarray,
    local_std: np.ndarray,
) -> dict:
    start_time = float(time[start_idx])
    end_time = float(time[end_idx])
    segment_values = values[start_idx : end_idx + 1]
    return {
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": float(end_time - start_time),
        "mean_value": float(np.nanmean(segment_values)),
        "min_value": float(np.nanmin(segment_values)),
        "max_value": float(np.nanmax(segment_values)),
        "mean_abs_slope": float(np.nanmean(np.abs(slope[start_idx : end_idx + 1]))),
        "max_change_in_window": float(np.nanmax(endpoint_change[start_idx : end_idx + 1])),
        "max_std_in_window": float(np.nanmax(local_std[start_idx : end_idx + 1])),
    }


def behavior_summary_row(sensor: str, summary: dict) -> dict:
    """Flatten a per-sensor behavior summary into one CSV-friendly row."""
    longest = summary.get("longest_steady_segment") or {}
    first_breakout = None
    transitions = summary.get("state_transitions") or []
    if transitions:
        first_breakout = transitions[0].get("time")
    return {
        "sensor": sensor,
        "n_samples": summary.get("n_samples"),
        "n_steady_segments": summary.get("n_steady_segments", 0),
        "total_steady_duration_seconds": summary.get("total_steady_duration_seconds", 0.0),
        "longest_steady_start_seconds": longest.get("start_time"),
        "longest_steady_end_seconds": longest.get("end_time"),
        "longest_steady_duration_seconds": longest.get("duration_seconds"),
        "first_breakout_seconds": first_breakout,
        "summary_text": summary.get("summary_text"),
    }


def _group_members(group_members: list[str], sensors: list[str]) -> list[str]:
    if any(member == "*" for member in group_members):
        return sensors
    selected = []
    available = {sensor.lower(): sensor for sensor in sensors}
    for member in group_members:
        sensor = available.get(str(member).lower())
        if sensor is not None:
            selected.append(sensor)
    return selected


def summarize_group_behaviors(sensor_details: dict[str, dict], behavior_cfg: dict | None = None) -> dict[str, dict]:
    """Build group-level steady-state summaries from per-sensor steady masks."""
    if not sensor_details:
        return {}
    cfg = _default_behavior_cfg()
    if behavior_cfg:
        cfg.update(behavior_cfg)
    group_map = cfg.get("groups") or {"all_sensors": ["*"]}
    sensors = sorted(sensor_details.keys())
    base_sensor = sensors[0]
    base_time = np.asarray(sensor_details[base_sensor].get("time", []), dtype=float)
    if base_time.size == 0:
        return {}

    summaries: dict[str, dict] = {}
    min_duration = float(cfg.get("min_steady_duration_seconds") or cfg["steady_window_seconds"])
    for group_name, members in group_map.items():
        resolved = _group_members(list(members), sensors)
        if not resolved:
            continue
        masks = [np.asarray(sensor_details[sensor]["steady_mask"], dtype=bool) for sensor in resolved]
        if any(mask.size != base_time.size for mask in masks):
            continue
        group_mask = np.logical_and.reduce(masks)
        steady_segments, transitions = _segments_from_mask(
            time=base_time,
            steady_mask=group_mask,
            build_segment=lambda start_idx, end_idx: _build_group_segment(base_time, start_idx, end_idx, resolved),
            min_duration=min_duration,
        )
        total_steady = float(sum(segment["duration_seconds"] for segment in steady_segments))
        longest = max(steady_segments, key=lambda segment: segment["duration_seconds"], default=None)
        first_breakout = transitions[0]["time"] if transitions else None
        if longest is None:
            summary_text = (
                f"Group `{group_name}` had no interval where all member sensors stayed approximately steady "
                f"for at least {min_duration:.0f} s."
            )
        else:
            summary_text = (
                f"Group `{group_name}` was simultaneously steady for {len(steady_segments)} window(s). "
                f"Longest: {_format_time(longest['start_time'])} to {_format_time(longest['end_time'])} "
                f"({longest['duration_seconds']:.2f} s)."
            )
            if first_breakout is not None:
                summary_text += f" First group breakout occurs near {_format_time(first_breakout)}."
        summaries[group_name] = {
            "group": group_name,
            "members": resolved,
            "n_samples": int(base_time.size),
            "n_steady_segments": len(steady_segments),
            "total_steady_duration_seconds": total_steady,
            "longest_steady_segment": longest,
            "steady_segments": steady_segments,
            "state_transitions": transitions,
            "summary_text": summary_text,
        }
    return summaries


def _build_group_segment(time: np.ndarray, start_idx: int, end_idx: int, members: list[str]) -> dict:
    start_time = float(time[start_idx])
    end_time = float(time[end_idx])
    return {
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": float(end_time - start_time),
        "members": members,
    }


def group_behavior_summary_row(group_name: str, summary: dict) -> dict:
    longest = summary.get("longest_steady_segment") or {}
    transitions = summary.get("state_transitions") or []
    first_breakout = transitions[0].get("time") if transitions else None
    return {
        "group": group_name,
        "members": ",".join(summary.get("members", [])),
        "n_samples": summary.get("n_samples"),
        "n_steady_segments": summary.get("n_steady_segments", 0),
        "total_steady_duration_seconds": summary.get("total_steady_duration_seconds", 0.0),
        "longest_steady_start_seconds": longest.get("start_time"),
        "longest_steady_end_seconds": longest.get("end_time"),
        "longest_steady_duration_seconds": longest.get("duration_seconds"),
        "first_breakout_seconds": first_breakout,
        "summary_text": summary.get("summary_text"),
    }


def write_behavior_outputs(
    output_dir: str | Path,
    sensor_summaries: dict[str, dict],
    *,
    group_summaries: dict[str, dict] | None = None,
) -> dict | None:
    """Write JSON, CSV, and Markdown behavior summaries for one clean run."""
    if not sensor_summaries:
        return None
    output_dir = Path(output_dir)
    sensors_dir = output_dir / "sensors"
    sensors_dir.mkdir(parents=True, exist_ok=True)

    rows = [behavior_summary_row(sensor, summary) for sensor, summary in sensor_summaries.items()]
    rows.sort(key=lambda row: row["sensor"])
    group_rows = [group_behavior_summary_row(name, summary) for name, summary in (group_summaries or {}).items()]
    group_rows.sort(key=lambda row: row["group"])

    summary_json = output_dir / "steady_state_summary.json"
    summary_csv = output_dir / "steady_state_summary.csv"
    group_summary_csv = output_dir / "steady_state_groups_summary.csv"
    report_md = output_dir / "steady_state_report.md"

    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "per_sensor": [
                    {"sensor": sensor, **summary}
                    for sensor, summary in sorted(sensor_summaries.items(), key=lambda item: item[0])
                ],
                "per_group": [
                    {"group": group_name, **summary}
                    for group_name, summary in sorted((group_summaries or {}).items(), key=lambda item: item[0])
                ],
            },
            f,
            indent=2,
        )
    write_csv_table(summary_csv, rows)
    if group_rows:
        write_csv_table(group_summary_csv, group_rows)

    for sensor, summary in sensor_summaries.items():
        sensor_path = sensors_dir / f"{normalize_header(sensor)}_steady_state.json"
        with open(sensor_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    report_lines = [
        "# Steady-State Report",
        "",
        "This report is a practical interpretation of the cleaned signals.",
        "A sensor is marked as steady when it changes only slightly across a sustained time window.",
        "",
    ]
    if group_rows:
        report_lines.extend(["## System / Group Summary", ""])
        for row in group_rows:
            report_lines.append(f"- `{row['group']}` ({row['members']}): {row['summary_text']}")
        report_lines.append("")
        report_lines.extend(["## Per-Sensor Summary", ""])
    for row in rows:
        report_lines.append(f"- `{row['sensor']}`: {row['summary_text']}")
    report_md.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    artifacts = {
        "steady_state_summary_json": str(summary_json),
        "steady_state_summary_csv": str(summary_csv),
        "steady_state_report_md": str(report_md),
    }
    if group_rows:
        artifacts["steady_state_groups_summary_csv"] = str(group_summary_csv)
    return artifacts
