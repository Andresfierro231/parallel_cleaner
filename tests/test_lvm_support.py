from __future__ import annotations
from pathlib import Path

import pandas as pd

from ncdt_cleaner.config import load_config
from ncdt_cleaner.inspectors import inspect_file
from ncdt_cleaner.normalization import dataframe_to_sensor_dataset
from ncdt_cleaner.readers import read_tabular_file


def _write_lvm(path: Path, contents: str) -> Path:
    path.write_text(contents, encoding="utf-8")
    return path


def test_read_lvm_single_block_with_explicit_x_values(tmp_path: Path) -> None:
    lvm_path = _write_lvm(
        tmp_path / "single_block.lvm",
        """LabVIEW Measurement
Writer_Version\t2
Reader_Version\t2
Separator\tTab
Decimal_Separator\t.
Multi_Headings\tNo
X_Columns\tOne
Time_Pref\tAbsolute
***End_of_Header***

Channels\t2
Samples\t3\t3
Date\t2026/02/19\t2026/02/19
Time\t12:12:00.0\t12:12:00.0
Y_Unit_Label\tDeg C\tDeg C
X_Dimension\tTime\tTime
X0\t0.0000000000000000E+0\t0.0000000000000000E+0
Delta_X\t0.500000\t0.500000
***End_of_Header***
X_Value\tTube_Inlet_1\tTube_Outlet_1\tComment
0.000000\t45.229875\t42.254468
0.500000\t45.230267\t42.254860
1.000000\t45.231000\t42.255400
""",
    )

    df = read_tabular_file(lvm_path)

    assert list(df.columns) == ["X_Value", "Tube_Inlet_1", "Tube_Outlet_1", "Comment"]
    assert df.shape == (3, 4)
    assert df["X_Value"].tolist() == [0.0, 0.5, 1.0]
    assert df["Tube_Inlet_1"].tolist() == [45.229875, 45.230267, 45.231]
    assert df.attrs["lvm_metadata"]["segment_count"] == 1


def test_read_lvm_reconstructs_time_and_concatenates_segments(tmp_path: Path) -> None:
    lvm_path = _write_lvm(
        tmp_path / "segmented.lvm",
        """LabVIEW Measurement
Writer_Version\t2
Reader_Version\t2
Separator\tTab
Decimal_Separator\t.
Multi_Headings\tYes
X_Columns\tNo
Time_Pref\tAbsolute
***End_of_Header***

Channels\t2
Samples\t2\t2
Date\t2026/04/02\t2026/04/02
Time\t14:14:48.0\t14:14:48.0
Y_Unit_Label\tDeg C\tDeg C
X_Dimension\tTime\tTime
X0\t0.0000000000000000E+0\t0.0000000000000000E+0
Delta_X\t0.500000\t0.500000
***End_of_Header***
X_Value\tTemperature_0\tTemperature_1\tComment
\t21.00\t20.90
\t21.10\t21.00

Channels\t2
Samples\t2\t2
Date\t2026/04/02\t2026/04/02
Time\t14:14:49.0\t14:14:49.0
Y_Unit_Label\tDeg C\tDeg C
X_Dimension\tTime\tTime
X0\t1.0000000000000000E+0\t1.0000000000000000E+0
Delta_X\t0.500000\t0.500000
***End_of_Header***
X_Value\tTemperature_0\tTemperature_1\tComment
\t21.20\t21.10
\t21.30\t21.20
""",
    )

    df = read_tabular_file(lvm_path)

    assert df["X_Value"].tolist() == [0.0, 0.5, 1.0, 1.5]
    assert df["Temperature_0"].tolist() == [21.0, 21.1, 21.2, 21.3]
    assert df.attrs["lvm_metadata"]["segment_count"] == 2
    assert df.attrs["lvm_metadata"]["segments"][1]["x0"] == "1.0000000000000000E+0"


def test_lvm_inspection_and_normalization_use_reconstructed_time(tmp_path: Path) -> None:
    lvm_path = _write_lvm(
        tmp_path / "workflow_ready.lvm",
        """LabVIEW Measurement
Writer_Version\t2
Reader_Version\t2
Separator\tTab
Decimal_Separator\t.
Multi_Headings\tYes
X_Columns\tNo
Time_Pref\tAbsolute
***End_of_Header***

Channels\t2
Samples\t2\t2
Date\t2026/04/02\t2026/04/02
Time\t14:14:48.0\t14:14:48.0
Y_Unit_Label\tDeg C\tDeg C
X_Dimension\tTime\tTime
X0\t0.0000000000000000E+0\t0.0000000000000000E+0
Delta_X\t1.000000\t1.000000
***End_of_Header***
X_Value\tTemperature_0\tTemperature_1\tComment
\t21.00\t20.90
\t21.10\t21.00

Channels\t2
Samples\t2\t2
Date\t2026/04/02\t2026/04/02
Time\t14:14:50.0\t14:14:50.0
Y_Unit_Label\tDeg C\tDeg C
X_Dimension\tTime\tTime
X0\t2.0000000000000000E+0\t2.0000000000000000E+0
Delta_X\t1.000000\t1.000000
***End_of_Header***
X_Value\tTemperature_0\tTemperature_1\tComment
\t21.20\t21.10
\t21.30\t21.20
""",
    )
    cfg = load_config("configs/default_config.json")

    inspection = inspect_file(lvm_path, cfg)
    df = read_tabular_file(lvm_path)
    dataset, summary = dataframe_to_sensor_dataset(df, "workflow_ready", cfg)

    assert inspection["suffix"] == ".lvm"
    assert inspection["lvm_metadata"]["segment_count"] == 2
    assert inspection["schema_mapping"]["time_column"]["original_name"] == "X_Value"
    assert dataset.time.tolist() == [0.0, 1.0, 2.0, 3.0]
    assert summary["n_sensors"] == 2
    assert summary["sensor_columns"] == ["Temperature_0", "Temperature_1"]


def test_real_vcu_lvm_sample_is_parseable() -> None:
    root = Path(__file__).resolve().parents[1]
    lvm_path = root / "data" / "vcu_data_deposit" / "MSETF-2-Forced_Convection_Data" / "02192026" / "Test00.lvm"
    if not lvm_path.exists():
        return

    df = read_tabular_file(lvm_path)

    assert isinstance(df, pd.DataFrame)
    assert "X_Value" in df.columns
    assert "Tube_Inlet_1" in df.columns
    assert len(df) > 0
    assert df.attrs["lvm_metadata"]["segment_count"] >= 1
