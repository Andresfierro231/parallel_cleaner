from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_config(path: str | None) -> dict[str, Any]:
    if path is None:
        path = "configs/default_config.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str | Path, data: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)
