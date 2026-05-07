from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _write_test_config(tmp_path: Path) -> Path:
    root = _root()
    payload = json.loads((root / "configs" / "default_config.json").read_text(encoding="utf-8"))
    payload["analysis_dir"] = str(tmp_path / "analysis")
    payload["benchmark"]["enabled_by_default"] = False
    payload["execution"]["default_modes"] = ["serial"]
    config_path = tmp_path / "test_config.json"
    config_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return config_path


def _subprocess_env(root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src = str(root / "src")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src if not existing else f"{src}:{existing}"
    env.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")
    return env


def _resolve_artifact(root: Path, path: str) -> Path:
    artifact = Path(path)
    return artifact if artifact.is_absolute() else root / artifact


def _run_workflow(tmp_path: Path, *extra_args: str) -> dict:
    root = _root()
    config_path = _write_test_config(tmp_path)
    input_path = root / "examples" / "tiny_sensor_example.csv"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ncdt_cleaner.cli",
            "--config",
            str(config_path),
            "workflow",
            str(input_path),
            "--characterize",
            *extra_args,
        ],
        check=True,
        cwd=root,
        capture_output=True,
        text=True,
        env=_subprocess_env(root),
    )
    return json.loads(proc.stdout)


def test_serial_workflow_creates_expected_outputs(tmp_path: Path) -> None:
    payload = _run_workflow(tmp_path)
    root = _root()

    assert payload["benchmark_enabled"] is False
    assert len(payload["clean_runs"]) == 1
    assert payload["clean_runs"][0]["mode"] == "serial"

    clean_run = payload["clean_runs"][0]
    output_dir = _resolve_artifact(root, clean_run["output_dir"])
    sensors_dir = output_dir / "sensors"

    assert output_dir.exists()
    assert _resolve_artifact(root, clean_run["steady_state_report_md"]).exists()
    assert _resolve_artifact(root, clean_run["steady_state_summary_csv"]).exists()
    assert _resolve_artifact(root, clean_run["characterization_summary_csv"]).exists()
    assert list(sensors_dir.glob("*_signal_overlay.png"))


def test_serial_workflow_skip_plots_omits_overlay_images(tmp_path: Path) -> None:
    payload = _run_workflow(tmp_path, "--skip-plots")
    root = _root()

    clean_run = payload["clean_runs"][0]
    output_dir = _resolve_artifact(root, clean_run["output_dir"])
    sensors_dir = output_dir / "sensors"

    assert output_dir.exists()
    assert _resolve_artifact(root, clean_run["characterization_summary_csv"]).exists()
    assert not list(sensors_dir.glob("*_signal_overlay.png"))
