from __future__ import annotations

from pathlib import Path
import tomllib


def test_pyproject_exposes_console_script_and_optional_parallel_extra() -> None:
    root = Path(__file__).resolve().parents[1]
    payload = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    project = payload["project"]
    dependencies = project["dependencies"]
    optional = project["optional-dependencies"]

    assert project["scripts"]["ncdt-cleaner"] == "ncdt_cleaner.cli:main"
    assert not any(dep.startswith("mpi4py") for dep in dependencies)
    assert "mpi4py>=3.1" in optional["parallel"]
    assert "pytest>=8.0" in optional["dev"]
