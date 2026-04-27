'''
File description:
Synthetic dataset generation helpers for smoke tests and scaling campaigns.

This module gives the project a reproducible way to create larger benchmark
inputs when a real dataset is too small for meaningful scaling measurements.
It intentionally supports a few messy-schema options as well so the report can
discuss both scaling and ingestion robustness without maintaining separate test
fixtures by hand.
'''

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import save_json
from .utils import ensure_dir


@dataclass
class SyntheticDatasetMetadata:
    """Describe one generated synthetic dataset for later benchmarking."""

    dataset_name: str
    path: str
    format: str
    n_rows: int
    n_sensors: int
    seed: int
    noise_sigma: float
    spike_fraction: float
    flat_fraction: float
    dropout_fraction: float
    time_mode: str
    header_style: str
    include_junk_columns: bool


def _time_values(n_rows: int, time_mode: str) -> tuple[np.ndarray | pd.DatetimeIndex, str]:
    """Build the time axis used for one synthetic dataset."""
    if time_mode == "numeric":
        return np.linspace(0.0, float(max(n_rows - 1, 0)), n_rows, dtype=float), "Time"
    if time_mode == "datetime":
        start = pd.Timestamp("2026-01-01T00:00:00")
        return pd.date_range(start=start, periods=n_rows, freq="s"), "Timestamp"
    if time_mode == "indexless":
        return np.linspace(0.0, float(max(n_rows - 1, 0)), n_rows, dtype=float), ""
    raise ValueError(f"Unsupported time_mode={time_mode}")


def _sensor_header(index: int, header_style: str) -> str:
    """Return one synthetic sensor header in the requested style."""
    if header_style == "standard":
        return f"Sensor_{index + 1}"
    if header_style == "irregular":
        patterns = [
            f"TC-{index + 1:02d} Temp (C)",
            f"flow sensor {index + 1}",
            f"Pressure.Channel.{index + 1}",
            f"sensor {index + 1} reading",
        ]
        return patterns[index % len(patterns)]
    raise ValueError(f"Unsupported header_style={header_style}")


def _apply_flat_segments(signal: np.ndarray, rng: np.random.Generator, flat_fraction: float) -> np.ndarray:
    """Flatten a few short regions to mimic sticky sensors or slow drift."""
    if flat_fraction <= 0.0 or signal.size == 0:
        return signal
    n_segments = max(1, min(10, int(signal.size * flat_fraction * 10) or 1))
    for _ in range(n_segments):
        start = int(rng.integers(0, max(signal.size - 2, 1)))
        length = int(rng.integers(3, max(4, min(signal.size // 50 + 1, 200))))
        end = min(signal.size, start + length)
        signal[start:end] = signal[start]
    return signal


def _apply_dropouts(signal: np.ndarray, rng: np.random.Generator, dropout_fraction: float) -> np.ndarray:
    """Inject missing values to stress NaN handling during cleaning."""
    if dropout_fraction <= 0.0 or signal.size == 0:
        return signal
    n_drop = int(signal.size * dropout_fraction)
    if n_drop <= 0:
        return signal
    idx = rng.choice(signal.size, size=n_drop, replace=False)
    signal[idx] = np.nan
    return signal


def generate_synthetic_timeseries(
    out_path: str | Path,
    n_rows: int = 200000,
    n_sensors: int = 8,
    spike_fraction: float = 0.002,
    seed: int = 1234,
    noise_sigma: float = 0.1,
    time_mode: str = "numeric",
    header_style: str = "standard",
    include_junk_columns: bool = False,
    flat_fraction: float = 0.0,
    dropout_fraction: float = 0.0,
    output_format: str | None = None,
) -> dict[str, Any]:
    """Generate a reproducible synthetic sensor dataset and save it to disk.

    Supported formats are currently CSV, JSON, NDJSON, and XLSX. CSV is the
    recommended default for large benchmark campaigns because it matches the
    course project emphasis on raw archival sensor files and scales more
    naturally to larger sizes than spreadsheet output.
    """
    out_path = Path(out_path)
    fmt = (output_format or out_path.suffix.lstrip(".")).lower() or "csv"
    rng = np.random.default_rng(seed)

    time_values, time_header = _time_values(n_rows=n_rows, time_mode=time_mode)
    frame_data: dict[str, Any] = {}
    if time_mode != "indexless":
        frame_data[time_header] = time_values

    base_time = np.linspace(0.0, 12.0 * np.pi, n_rows, dtype=float) if n_rows > 0 else np.array([], dtype=float)
    trend_axis = np.linspace(0.0, 1.0, n_rows, dtype=float) if n_rows > 0 else np.array([], dtype=float)

    for sensor_index in range(n_sensors):
        header = _sensor_header(sensor_index, header_style=header_style)
        amplitude = 1.0 + 0.15 * sensor_index
        angular = 0.6 + 0.07 * sensor_index
        phase = 0.5 * sensor_index
        baseline = amplitude * np.sin(base_time * angular / (2.0 * np.pi) + phase)
        seasonal = 0.35 * np.cos(base_time * (0.18 + 0.01 * sensor_index) + 0.3 * sensor_index)
        drift = (0.5 + 0.05 * sensor_index) * trend_axis
        signal = baseline + seasonal + drift + noise_sigma * rng.standard_normal(n_rows)

        n_spikes = int(n_rows * spike_fraction)
        if n_spikes > 0 and n_rows > 0:
            idx = rng.choice(n_rows, size=n_spikes, replace=False)
            signal[idx] += rng.normal(loc=0.0, scale=max(6.0, 20.0 * noise_sigma), size=n_spikes)

        signal = _apply_flat_segments(signal, rng=rng, flat_fraction=flat_fraction)
        signal = _apply_dropouts(signal, rng=rng, dropout_fraction=dropout_fraction)
        frame_data[header] = signal

    if include_junk_columns:
        frame_data["Operator Notes"] = np.where((np.arange(n_rows) % 97) == 0, "check sensor bank", "ok")
        frame_data["Batch ID"] = np.repeat(f"run-{seed}", n_rows)
        frame_data["QC Flag"] = np.where((np.arange(n_rows) % 211) == 0, "WARN", "PASS")

    df = pd.DataFrame(frame_data)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(out_path, index=False)
    elif fmt == "json":
        df.to_json(out_path, orient="records", indent=2, date_format="iso")
    elif fmt == "ndjson":
        df.to_json(out_path, orient="records", lines=True, date_format="iso")
    elif fmt == "xlsx":
        df.to_excel(out_path, index=False)
    else:
        raise ValueError(f"Unsupported synthetic output format: {fmt}")

    metadata = SyntheticDatasetMetadata(
        dataset_name=out_path.stem,
        path=str(out_path),
        format=fmt,
        n_rows=int(n_rows),
        n_sensors=int(n_sensors),
        seed=int(seed),
        noise_sigma=float(noise_sigma),
        spike_fraction=float(spike_fraction),
        flat_fraction=float(flat_fraction),
        dropout_fraction=float(dropout_fraction),
        time_mode=time_mode,
        header_style=header_style,
        include_junk_columns=bool(include_junk_columns),
    )
    return {"path": out_path, "metadata": asdict(metadata)}


def create_synthetic_campaign(
    out_dir: str | Path,
    campaign_name: str,
    row_counts: list[int],
    num_sensors: int,
    noise_sigma: float,
    spike_fraction: float,
    seed_base: int,
    *,
    time_mode: str = "numeric",
    header_style: str = "standard",
    include_junk_columns: bool = False,
    flat_fraction: float = 0.0,
    dropout_fraction: float = 0.0,
    formats: list[str] | tuple[str, ...] = ("csv",),
    dry_run: bool = False,
) -> dict[str, Any]:
    """Generate a family of benchmark datasets plus a manifest."""
    out_dir = ensure_dir(out_dir)
    datasets_dir = ensure_dir(out_dir / "datasets")
    rows: list[dict[str, Any]] = []

    for idx, n_rows in enumerate(row_counts):
        for fmt in formats:
            dataset_name = f"{campaign_name}_rows_{int(n_rows)}_sensors_{int(num_sensors)}_seed_{int(seed_base + idx)}.{fmt}"
            out_path = datasets_dir / dataset_name
            metadata = {
                "dataset_name": out_path.stem,
                "path": str(out_path),
                "format": fmt,
                "n_rows": int(n_rows),
                "n_sensors": int(num_sensors),
                "seed": int(seed_base + idx),
                "noise_sigma": float(noise_sigma),
                "spike_fraction": float(spike_fraction),
                "flat_fraction": float(flat_fraction),
                "dropout_fraction": float(dropout_fraction),
                "time_mode": time_mode,
                "header_style": header_style,
                "include_junk_columns": bool(include_junk_columns),
            }
            if not dry_run:
                result = generate_synthetic_timeseries(
                    out_path=out_path,
                    n_rows=int(n_rows),
                    n_sensors=int(num_sensors),
                    spike_fraction=float(spike_fraction),
                    seed=int(seed_base + idx),
                    noise_sigma=float(noise_sigma),
                    time_mode=time_mode,
                    header_style=header_style,
                    include_junk_columns=bool(include_junk_columns),
                    flat_fraction=float(flat_fraction),
                    dropout_fraction=float(dropout_fraction),
                    output_format=fmt,
                )
                metadata = result["metadata"]
            rows.append(metadata)

    manifest = {
        "campaign_name": campaign_name,
        "out_dir": str(out_dir),
        "datasets_dir": str(datasets_dir),
        "row_counts": [int(v) for v in row_counts],
        "num_sensors": int(num_sensors),
        "noise_sigma": float(noise_sigma),
        "spike_fraction": float(spike_fraction),
        "flat_fraction": float(flat_fraction),
        "dropout_fraction": float(dropout_fraction),
        "seed_base": int(seed_base),
        "time_mode": time_mode,
        "header_style": header_style,
        "include_junk_columns": bool(include_junk_columns),
        "formats": list(formats),
        "dry_run": bool(dry_run),
        "datasets": rows,
    }

    manifest_path = out_dir / "campaign_manifest.json"
    summary_csv = out_dir / "campaign_summary.csv"
    if not dry_run:
        save_json(manifest_path, manifest)
        pd.DataFrame(rows).to_csv(summary_csv, index=False)
    return {
        "campaign_manifest_json": str(manifest_path),
        "campaign_summary_csv": str(summary_csv),
        "datasets": rows,
        "out_dir": str(out_dir),
        "datasets_dir": str(datasets_dir),
        "dry_run": bool(dry_run),
    }
