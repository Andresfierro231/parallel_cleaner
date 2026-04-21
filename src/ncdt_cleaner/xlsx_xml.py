from __future__ import annotations

import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def _col_index(cell_ref: str) -> int:
    letters = re.match(r"([A-Z]+)", cell_ref).group(1)
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - 64)
    return idx - 1


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall("a:si", NS):
        text = "".join(node.text or "" for node in si.findall(".//a:t", NS))
        out.append(text)
    return out


def list_sheets(path: str | Path) -> list[str]:
    with zipfile.ZipFile(path) as zf:
        wb = ET.fromstring(zf.read("xl/workbook.xml"))
        return [sh.attrib["name"] for sh in wb.find("a:sheets", NS)]


def _resolve_sheet_target(zf: zipfile.ZipFile, sheet_name: str | None) -> tuple[str, str]:
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    sheets = wb.find("a:sheets", NS)
    if sheets is None or not list(sheets):
        raise ValueError("Workbook does not contain sheets")
    selected = None
    if sheet_name is None:
        selected = list(sheets)[0]
    else:
        for sh in sheets:
            if sh.attrib["name"] == sheet_name:
                selected = sh
                break
    if selected is None:
        raise ValueError(f"Sheet not found: {sheet_name}")
    rid = selected.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
    target = rel_map[rid]
    if not target.startswith("xl/"):
        target = "xl/" + target
    return selected.attrib["name"], target


def read_sheet(path: str | Path, sheet_name: str | None = None) -> pd.DataFrame:
    with zipfile.ZipFile(path) as zf:
        shared = _load_shared_strings(zf)
        selected_name, target = _resolve_sheet_target(zf, sheet_name)
        root = ET.fromstring(zf.read(target))
        sheet_data = root.find("a:sheetData", NS)
        rows: list[list[str | None]] = []
        max_col = 0
        if sheet_data is not None:
            for row in sheet_data.findall("a:row", NS):
                values: dict[int, str | None] = {}
                for cell in row.findall("a:c", NS):
                    ref = cell.attrib.get("r", "A1")
                    idx = _col_index(ref)
                    max_col = max(max_col, idx + 1)
                    cell_type = cell.attrib.get("t")
                    v = cell.find("a:v", NS)
                    if v is not None:
                        raw = v.text
                        if cell_type == "s" and raw is not None:
                            value = shared[int(raw)]
                        else:
                            value = raw
                    else:
                        inline = cell.find("a:is", NS)
                        if inline is None:
                            value = None
                        else:
                            value = "".join(node.text or "" for node in inline.findall(".//a:t", NS))
                    values[idx] = value
                rows.append([values.get(i) for i in range(max_col)])
        if not rows:
            return pd.DataFrame()
        header = [str(h) if h is not None else f"unnamed_{i}" for i, h in enumerate(rows[0])]
        body = rows[1:]
        df = pd.DataFrame(body, columns=header)
        df.attrs["sheet_name"] = selected_name
        return df


def quick_inspect(path: str | Path, sheet_name: str | None = None, max_rows: int = 5) -> dict:
    df = read_sheet(path, sheet_name=sheet_name)
    return {
        "sheet_name": df.attrs.get("sheet_name", sheet_name),
        "columns": df.columns.tolist(),
        "sample_rows": df.head(max_rows).to_dict(orient="records"),
        "shape": [int(df.shape[0]), int(df.shape[1])],
    }
