#!/usr/bin/env python3
"""
Build a UTF-8 JSON dataset from the official CSV files under `.context/`.

Input files (expected MacRoman encoding, semicolon-separated, CRLF):
- .context/municipios.csv
- .context/codprov.csv

Output file (UTF-8):
- src/data/municipalities.json
"""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

MUNICIPIOS_CSV = ROOT / ".context" / "municipios.csv"
PROVINCIAS_CSV = ROOT / ".context" / "codprov.csv"
OUT_JSON = ROOT / "src" / "data" / "municipalities.json"


def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # keep letters/numbers/spaces; convert separators to spaces
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_provinces() -> dict[str, str]:
    """
    Returns {CPRO: province_name}
    """
    data = PROVINCIAS_CSV.read_text(encoding="mac_roman", newline="")
    lines = data.splitlines()
    # Skip the first descriptive line
    # Header line: CODIGO;LITERAL
    reader = csv.DictReader(lines[1:], delimiter=";")
    out: dict[str, str] = {}
    for row in reader:
        code = (row.get("CODIGO") or "").strip()
        name = (row.get("LITERAL") or "").strip()
        if code and name:
            out[code.zfill(2)] = name
    return out


def read_municipalities(provinces: dict[str, str]) -> list[dict]:
    data = MUNICIPIOS_CSV.read_text(encoding="mac_roman", newline="")
    lines = data.splitlines()
    # Skip the first descriptive line
    # Header line: CODAUTO;CPRO;CMUN;DC;NOMBRE
    reader = csv.DictReader(lines[1:], delimiter=";")

    out: list[dict] = []
    for row in reader:
        cauto = (row.get("CODAUTO") or "").strip()
        cpro = (row.get("CPRO") or "").strip().zfill(2)
        cmun = (row.get("CMUN") or "").strip().zfill(3)
        dc = (row.get("DC") or "").strip()
        name = (row.get("NOMBRE") or "").strip()
        if not (cpro and cmun and dc and name):
            continue
        prov_name = provinces.get(cpro, "")
        municipality_id = f"{cpro}-{cmun}-{dc}"
        out.append(
            {
                "id": municipality_id,
                "cauto": cauto,
                "cpro": cpro,
                "cmun": cmun,
                "dc": dc,
                "name": name,
                "name_normalized": normalize_text(name),
                "province_name": prov_name,
                "province_name_normalized": normalize_text(prov_name) if prov_name else "",
            }
        )
    return out


def main() -> int:
    if not MUNICIPIOS_CSV.exists():
        raise SystemExit(f"Missing {MUNICIPIOS_CSV}")
    if not PROVINCIAS_CSV.exists():
        raise SystemExit(f"Missing {PROVINCIAS_CSV}")

    provinces = read_provinces()
    municipalities = read_municipalities(provinces)
    municipalities.sort(key=lambda m: (m["province_name"], m["name"]))

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(
            {
                "source": {
                    "municipios_csv": str(MUNICIPIOS_CSV.relative_to(ROOT)),
                    "codprov_csv": str(PROVINCIAS_CSV.relative_to(ROOT)),
                },
                "count": len(municipalities),
                "items": municipalities,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUT_JSON} ({len(municipalities)} items)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

