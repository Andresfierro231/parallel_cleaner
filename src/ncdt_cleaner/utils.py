'''
File description:
General utility helpers shared by multiple project modules.

The functions here are intentionally small and boring: path creation, header
normalization, timestamps, host metadata, and lightweight git provenance.
'''

from __future__ import annotations

import datetime as _dt
import os
import re
import socket
import subprocess
from pathlib import Path


def now_utc_iso() -> str:
    """Return the current UTC timestamp as a compact ISO string."""
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def today_str() -> str:
    """Return today's UTC date in `YYYY-MM-DD` format."""
    return _dt.datetime.utcnow().date().isoformat()


def short_id(n: int = 8) -> str:
    """Generate a short random hexadecimal identifier."""
    import uuid
    return uuid.uuid4().hex[:n]


def normalize_header(name: str) -> str:
    """Normalize a raw header into a lowercase underscore identifier."""
    cleaned = str(name).strip().lower()
    cleaned = cleaned.replace("°", "deg")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def try_git_commit(cwd: str | os.PathLike[str] | None = None) -> str | None:
    """Best-effort lookup of the current git commit hash."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def host_info() -> dict:
    """Return lightweight host metadata for session logs."""
    return {
        "hostname": socket.gethostname(),
        "platform": os.uname().sysname if hasattr(os, "uname") else os.name,
    }


def ensure_dir(path: str | os.PathLike[str]) -> Path:
    """Create a directory if needed and return it as a `Path` object."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
