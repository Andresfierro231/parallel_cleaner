'''
File description:
Session-management helpers for creating dated analysis folders and logging.

Every CLI command writes into its own session directory so experiments remain
traceable and users can compare outputs without manually organizing files.
'''

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from .config import save_json
from .utils import ensure_dir, host_info, now_utc_iso, short_id, today_str, try_git_commit


def create_session(analysis_dir: str | Path, command_name: str, argv: list[str]) -> dict[str, Any]:
    """Create a dated output directory tree and its session metadata."""
    analysis_dir = ensure_dir(analysis_dir)
    sid = f"{today_str()}_session_{short_id()}"
    session_dir = ensure_dir(Path(analysis_dir) / sid)
    logs_dir = ensure_dir(session_dir / "logs")
    outputs_dir = ensure_dir(session_dir / "outputs")
    plots_dir = ensure_dir(session_dir / "plots")
    cache_dir = ensure_dir(session_dir / "cache")
    meta = {
        "session_id": sid,
        "command_name": command_name,
        "timestamp_utc": now_utc_iso(),
        "argv": argv,
        "git_commit": try_git_commit(),
        **host_info(),
        "paths": {
            "session_dir": str(session_dir),
            "logs_dir": str(logs_dir),
            "outputs_dir": str(outputs_dir),
            "plots_dir": str(plots_dir),
            "cache_dir": str(cache_dir),
        },
    }
    save_json(session_dir / "session_metadata.json", meta)
    configure_logging(logs_dir / "session.log")
    logging.getLogger(__name__).info("Created session %s", sid)
    return meta


def configure_logging(log_path: str | Path) -> None:
    """Configure file and console logging for the active session."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )
