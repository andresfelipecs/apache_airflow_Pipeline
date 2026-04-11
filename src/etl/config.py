from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
EXPORTS_DIR = DATA_DIR / "exports"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
REPORTS_DIR = PROJECT_ROOT / "reports"
SQL_DIR = PROJECT_ROOT / "sql"


def _first_existing_path(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


def _resolve_input_path(env_name: str, candidates: list[Path]) -> Path:
    configured = os.getenv(env_name)
    if configured:
        return Path(configured).expanduser().resolve()

    return _first_existing_path(candidates)


@dataclass(frozen=True)
class Settings:
    spotify_csv_path: Path
    grammy_csv_path: Path
    source_db_path: Path
    source_table_name: str
    warehouse_db_path: Path
    local_drive_export_dir: Path
    staging_dir: Path
    reports_dir: Path
    warehouse_schema_path: Path


def get_settings() -> Settings:
    spotify_candidates = [
        RAW_DIR / "spotify_dataset.csv",
        Path.home() / "Downloads" / "spotify_dataset.csv",
    ]
    grammy_candidates = [
        RAW_DIR / "the_grammy_awards.csv",
        Path.home() / "Downloads" / "the_grammy_awards.csv",
    ]

    source_db_path = Path(
        os.getenv("GRAMMY_SOURCE_DB_PATH", WAREHOUSE_DIR / "grammy_source.sqlite")
    ).expanduser()
    warehouse_db_path = Path(
        os.getenv("WAREHOUSE_DB_PATH", WAREHOUSE_DIR / "spotify_grammy_dw.sqlite")
    ).expanduser()
    local_drive_export_dir = Path(
        os.getenv("LOCAL_DRIVE_EXPORT_DIR", EXPORTS_DIR / "google_drive")
    ).expanduser()

    return Settings(
        spotify_csv_path=_resolve_input_path("SPOTIFY_CSV_PATH", spotify_candidates),
        grammy_csv_path=_resolve_input_path("GRAMMY_CSV_PATH", grammy_candidates),
        source_db_path=source_db_path,
        source_table_name=os.getenv("GRAMMY_SOURCE_TABLE", "grammy_awards"),
        warehouse_db_path=warehouse_db_path,
        local_drive_export_dir=local_drive_export_dir,
        staging_dir=STAGING_DIR,
        reports_dir=REPORTS_DIR,
        warehouse_schema_path=SQL_DIR / "warehouse_schema.sql",
    )

