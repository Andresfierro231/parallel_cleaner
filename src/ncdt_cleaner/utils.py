from __future__ import annotations

import datetime as _dt
import os
import re
import socket
import subprocess
from pathlib import Path


def now_utc_iso() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def today_str() -> str:
    return _dt.datetime.utcnow().date().isoformat()


def short_id(n: int = 8) -> str:
    import uuid
    return uuid.uuid4().hex[:n]


def normalize_header(name: str) -> str:
    cleaned = str(name).strip().lower()
    cleaned = cleaned.replace("°", "deg")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def try_git_commit(cwd: str | os.PathLike[str] | None = None) -> str | None:
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
    return {
        "hostname": socket.gethostname(),
        "platform": os.uname().sysname if hasattr(os, "uname") else os.name,
    }


def ensure_dir(path: str | os.PathLike[str]) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
