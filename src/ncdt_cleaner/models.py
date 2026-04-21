from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass
class TimeColumnInference:
    original_name: str | None
    normalized_name: str | None
    confidence: float
    candidate_scores: dict[str, float]
    reasoning: list[str] = field(default_factory=list)
    ambiguous: bool = False


@dataclass
class SchemaMapping:
    original_headers: list[str]
    normalized_headers: list[str]
    time_column: TimeColumnInference
    sensor_columns: list[str]
    sensor_columns_normalized: list[str]
    excluded_columns: list[str]
    notes: list[str] = field(default_factory=list)


@dataclass
class SensorDataset:
    name: str
    time: np.ndarray
    sensors: dict[str, np.ndarray]
    metadata: dict[str, Any] = field(default_factory=dict)

    def sensor_names(self) -> list[str]:
        return list(self.sensors.keys())

    def n_rows(self) -> int:
        return int(self.time.shape[0]) if self.time is not None else 0
