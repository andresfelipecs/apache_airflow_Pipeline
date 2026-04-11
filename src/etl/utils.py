from __future__ import annotations

import csv
import json
import re
import shutil
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Iterable


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def write_csv_rows(
    path: Path, rows: Iterable[dict], fieldnames: list[str] | None = None
) -> Path:
    rows = list(rows)
    ensure_directory(path.parent)

    if not rows and not fieldnames:
        raise ValueError(f"Cannot write empty CSV without fieldnames: {path}")

    if fieldnames is None:
        fieldnames = list(rows[0].keys())

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return path


def write_json(path: Path, payload: dict) -> Path:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return path


def copy_file(source: Path, target: Path) -> Path:
    ensure_directory(target.parent)
    shutil.copy2(source, target)
    return target


def normalize_text(value: str | None) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\b(feat|featuring|ft)\b\.?", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def split_primary_artist(artists: str | None) -> str:
    artists = (artists or "").strip()
    if not artists:
        return ""

    tokens = re.split(
        r"\s*(?:;|,|&|\bx\b|\bfeat\b\.?|\bfeaturing\b|\bft\b\.?)\s*",
        artists,
        maxsplit=1,
        flags=re.IGNORECASE,
    )
    return tokens[0].strip()


def parse_bool(value: str | bool | int | None) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return 1 if value else 0

    normalized = (value or "").strip().lower()
    return 1 if normalized in {"true", "1", "yes", "y"} else 0


def parse_int(value: str | int | None, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def parse_float(value: str | float | None, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def popularity_bucket(popularity: int) -> str:
    if popularity >= 80:
        return "blockbuster"
    if popularity >= 60:
        return "high"
    if popularity >= 40:
        return "medium"
    if popularity >= 20:
        return "emerging"
    return "niche"


def unique_join(values: Iterable[str], separator: str = " | ") -> str:
    normalized = sorted({value for value in values if value})
    return separator.join(normalized)


def rows_to_lookup(rows: Iterable[dict], key: str) -> dict[str, list[dict]]:
    lookup: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        lookup[row[key]].append(row)
    return dict(lookup)


def sqlite_connect(path: Path) -> sqlite3.Connection:
    ensure_directory(path.parent)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection
