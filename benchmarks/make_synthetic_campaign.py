"""Create a reproducible synthetic benchmark campaign for the NCDT project."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ncdt_cleaner.config import load_config
from ncdt_cleaner.synthetic import create_synthetic_campaign


DEFAULT_CAMPAIGN_CONFIG = ROOT / "configs" / "benchmark_campaigns" / "default_campaign.json"


def _load_campaign_config(path: str | None) -> dict:
    if path is None:
        path = str(DEFAULT_CAMPAIGN_CONFIG)
    return load_config(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a family of synthetic benchmark inputs")
    parser.add_argument("--config", default=str(DEFAULT_CAMPAIGN_CONFIG), help="JSON campaign config file")
    parser.add_argument("--campaign-name", default=None)
    parser.add_argument("--outdir", default=None)
    parser.add_argument("--rows", nargs="+", type=int, default=None)
    parser.add_argument("--num-sensors", type=int, default=None)
    parser.add_argument("--noise-sigma", type=float, default=None)
    parser.add_argument("--spike-fraction", type=float, default=None)
    parser.add_argument("--flat-fraction", type=float, default=None)
    parser.add_argument("--dropout-fraction", type=float, default=None)
    parser.add_argument("--seed-base", type=int, default=None)
    parser.add_argument("--time-mode", choices=["numeric", "datetime", "indexless"], default=None)
    parser.add_argument("--header-style", choices=["standard", "irregular"], default=None)
    parser.add_argument("--include-junk-columns", action="store_true")
    parser.add_argument("--formats", nargs="+", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = _load_campaign_config(args.config)
    campaign_name = args.campaign_name or cfg.get("campaign_name", "scaling_default")
    outdir = Path(args.outdir) if args.outdir is not None else Path(cfg.get("outdir", "analysis/synthetic_campaigns")) / campaign_name

    report = create_synthetic_campaign(
        out_dir=outdir,
        campaign_name=campaign_name,
        row_counts=[int(v) for v in (args.rows or cfg.get("row_counts", [100000, 500000, 1000000, 5000000]))],
        num_sensors=int(args.num_sensors if args.num_sensors is not None else cfg.get("num_sensors", 8)),
        noise_sigma=float(args.noise_sigma if args.noise_sigma is not None else cfg.get("noise_sigma", 0.35)),
        spike_fraction=float(args.spike_fraction if args.spike_fraction is not None else cfg.get("spike_fraction", 0.002)),
        seed_base=int(args.seed_base if args.seed_base is not None else cfg.get("seed_base", 100)),
        time_mode=args.time_mode or cfg.get("time_mode", "numeric"),
        header_style=args.header_style or cfg.get("header_style", "standard"),
        include_junk_columns=bool(args.include_junk_columns or cfg.get("include_junk_columns", False)),
        flat_fraction=float(args.flat_fraction if args.flat_fraction is not None else cfg.get("flat_fraction", 0.0005)),
        dropout_fraction=float(args.dropout_fraction if args.dropout_fraction is not None else cfg.get("dropout_fraction", 0.0002)),
        formats=tuple(args.formats or cfg.get("formats", ["csv"])),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
