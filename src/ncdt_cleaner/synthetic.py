from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def generate_synthetic_timeseries(
    out_path: str | Path,
    n_rows: int = 200000,
    n_sensors: int = 8,
    spike_fraction: float = 0.002,
    seed: int = 1234,
) -> Path:
    out_path = Path(out_path)
    rng = np.random.default_rng(seed)
    time = np.linspace(0.0, 1000.0, n_rows)
    data = {"Time": time}
    for i in range(n_sensors):
        base = np.sin(time * (0.01 + 0.001 * i)) + 0.1 * rng.standard_normal(n_rows)
        drift = 0.001 * i * time
        signal = base + drift
        n_spikes = int(n_rows * spike_fraction)
        idx = rng.choice(n_rows, size=n_spikes, replace=False)
        signal[idx] += rng.normal(0.0, 8.0, size=n_spikes)
        data[f"Sensor_{i+1}"] = signal
    df = pd.DataFrame(data)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path
