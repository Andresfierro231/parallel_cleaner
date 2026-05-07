import numpy as np

from ncdt_cleaner.behavior import analyze_signal_behavior, summarize_group_behaviors, summarize_signal_behavior


def test_behavior_summary_detects_steady_segment_and_breakout():
    time = np.arange(0.0, 180.0, 1.0)
    values = np.concatenate(
        [
            np.full(70, 10.0),
            np.linspace(10.0, 20.0, 40),
            np.full(time.size - 110, 20.0),
        ]
    )

    summary = summarize_signal_behavior(
        time,
        values,
        {
            "steady_window_seconds": 30.0,
            "min_steady_duration_seconds": 20.0,
            "change_fraction_of_scale": 0.08,
            "std_fraction_of_scale": 0.05,
        },
    )

    assert summary["n_steady_segments"] >= 2
    assert summary["longest_steady_segment"] is not None
    assert summary["state_transitions"]
    assert "steady-state" in summary["summary_text"].lower()
    assert summary["state_transitions"][0]["from_state"] == "steady"
    assert summary["state_transitions"][0]["to_state"] == "changing"


def test_group_summary_requires_all_member_sensors_to_be_steady():
    time = np.arange(0.0, 180.0, 1.0)
    sensor_a = np.concatenate(
        [
            np.full(70, 10.0),
            np.linspace(10.0, 20.0, 40),
            np.full(time.size - 110, 20.0),
        ]
    )
    sensor_b = np.concatenate(
        [
            np.full(90, 5.0),
            np.linspace(5.0, 8.0, 30),
            np.full(time.size - 120, 8.0),
        ]
    )
    cfg = {
        "steady_window_seconds": 30.0,
        "min_steady_duration_seconds": 20.0,
        "change_fraction_of_scale": 0.08,
        "std_fraction_of_scale": 0.05,
        "groups": {
            "all_sensors": ["*"],
            "pair": ["sensor_a", "sensor_b"],
        },
    }
    _, detail_a = analyze_signal_behavior(time, sensor_a, cfg)
    _, detail_b = analyze_signal_behavior(time, sensor_b, cfg)
    groups = summarize_group_behaviors({"sensor_a": detail_a, "sensor_b": detail_b}, cfg)

    assert "all_sensors" in groups
    assert "pair" in groups
    assert groups["all_sensors"]["n_steady_segments"] >= 1
    assert groups["all_sensors"]["members"] == ["sensor_a", "sensor_b"]
    assert "simultaneously steady" in groups["all_sensors"]["summary_text"]
