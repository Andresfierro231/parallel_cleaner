from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Iterator

import h5py
import numpy as np
import pandas as pd

from .xlsx_xml import read_sheet

LOGGER = logging.getLogger(__name__)


def detect_csv_encoding(path: str | Path) -> str:
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "latin1"


def read_csv_frame(path: str | Path, chunksize: int | None = None) -> pd.DataFrame | Iterator[pd.DataFrame]:
    enc = detect_csv_encoding(path)
    LOGGER.info("Reading CSV %s with encoding=%s", path, enc)
    return pd.read_csv(path, encoding=enc, chunksize=chunksize)


def read_json_frame(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".ndjson":
        return pd.read_json(path, lines=True)
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    if isinstance(obj, dict):
        if "records" in obj and isinstance(obj["records"], list):
            return pd.json_normalize(obj["records"])
        return pd.json_normalize([obj])
    raise ValueError(f"Unsupported JSON structure in {path}")


def read_hdf5_frame(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    with h5py.File(path, "r") as h5:
        if "table" in h5:
            dset = h5["table"]
            cols = [c.decode("utf-8") if isinstance(c, bytes) else str(c) for c in dset.attrs.get("columns", [])]
            arr = dset[...]
            return pd.DataFrame(arr, columns=cols if cols else None)
        columns = {}
        for key, value in h5.items():
            if isinstance(value, h5py.Dataset) and value.ndim == 1:
                columns[key] = value[...]
        if columns:
            return pd.DataFrame(columns)
    raise ValueError(f"Could not infer a tabular HDF5 layout from {path}")


def read_tabular_file(path: str | Path, sheet_name: str | None = None) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_frame(path)
    if suffix == ".xlsx":
        return read_sheet(path, sheet_name=sheet_name)
    if suffix in {".json", ".ndjson"}:
        return read_json_frame(path)
    if suffix in {".h5", ".hdf5"}:
        return read_hdf5_frame(path)
    raise ValueError(f"Unsupported file type: {suffix}")


def estimate_csv_rows(path: str | Path) -> int | None:
    try:
        enc = detect_csv_encoding(path)
        with open(path, "r", encoding=enc, errors="replace") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:
        return None
