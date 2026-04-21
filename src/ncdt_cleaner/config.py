'''
File description:
Configuration file helpers for reading JSON settings and writing JSON outputs.

The project keeps configuration intentionally simple so students can inspect and
edit plain JSON without learning a more elaborate configuration framework.
'''

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_config(path: str | None) -> dict[str, Any]:
    """Load a JSON configuration file into a Python dictionary."""
    if path is None:
        path = "configs/default_config.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str | Path, data: dict) -> None:
    """Write a JSON dictionary to disk, creating parent directories as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)
