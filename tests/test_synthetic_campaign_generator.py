from __future__ import annotations

import json
from pathlib import Path

from ncdt_cleaner.synthetic import create_synthetic_campaign, generate_synthetic_timeseries


def test_generate_synthetic_timeseries_supports_irregular_headers(tmp_path: Path) -> None:
    out = tmp_path / "irregular_case.csv"
    result = generate_synthetic_timeseries(
        out_path=out,
        n_rows=128,
        n_sensors=4,
        seed=7,
        time_mode="datetime",
        header_style="irregular",
        include_junk_columns=True,
    )
    assert out.exists()
    assert result["metadata"]["header_style"] == "irregular"
    assert result["metadata"]["time_mode"] == "datetime"


def test_create_synthetic_campaign_writes_manifest_and_summary(tmp_path: Path) -> None:
    report = create_synthetic_campaign(
        out_dir=tmp_path / "campaign",
        campaign_name="tiny_scaling",
        row_counts=[32, 64],
        num_sensors=3,
        noise_sigma=0.2,
        spike_fraction=0.01,
        seed_base=10,
        formats=("csv",),
    )
    manifest_path = Path(report["campaign_manifest_json"])
    summary_path = Path(report["campaign_summary_csv"])
    assert manifest_path.exists()
    assert summary_path.exists()

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["campaign_name"] == "tiny_scaling"
    assert len(payload["datasets"]) == 2
    assert all(Path(row["path"]).exists() for row in payload["datasets"])
