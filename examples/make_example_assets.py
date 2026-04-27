"""Generate small tracked example assets used in the README."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent


def render_steady_state_report_screenshot() -> Path:
    lines = [
        "# Steady-State Report",
        "",
        "This report is a practical interpretation of the cleaned signals.",
        "A sensor is marked as steady when it changes only slightly",
        "across a sustained time window.",
        "",
        "- Sensor_A: Detected 1 steady-state window.",
        "  Longest: 190 s to 310 s (120 s).",
        "  First breakout: near 320 s.",
        "",
        "- Sensor_B: Detected 2 steady-state windows.",
        "  Longest: 30 s to 120 s (90 s).",
        "  First breakout: near 130 s.",
        "",
        "- Sensor_C: No sustained steady-state window",
        "  of at least 60 s was detected.",
    ]

    fig = plt.figure(figsize=(9.5, 5.5))
    ax = fig.add_subplot(111)
    ax.set_facecolor("#f6f8fa")
    fig.patch.set_facecolor("white")
    ax.axis("off")
    ax.text(
        0.03,
        0.97,
        "\n".join(lines),
        va="top",
        ha="left",
        family="monospace",
        fontsize=12,
        color="#24292f",
    )
    out_path = ROOT / "steady_state_report_example.png"
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> int:
    render_steady_state_report_screenshot()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
