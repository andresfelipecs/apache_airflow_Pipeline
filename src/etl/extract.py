from __future__ import annotations

from pathlib import Path

from .config import Settings
from .utils import (
    ensure_directory,
    read_csv_rows,
    sqlite_connect,
    write_csv_rows,
)


GRAMMY_SOURCE_COLUMNS = [
    "year",
    "title",
    "published_at",
    "updated_at",
    "category",
    "nominee",
    "artist",
    "workers",
    "img",
    "winner",
]


def bootstrap_grammy_source_db(settings: Settings) -> Path:
    raw_rows = read_csv_rows(settings.grammy_csv_path)
    ensure_directory(settings.source_db_path.parent)

    with sqlite_connect(settings.source_db_path) as connection:
        connection.execute(f"DROP TABLE IF EXISTS {settings.source_table_name}")
        connection.execute(
            f"""
            CREATE TABLE {settings.source_table_name} (
                year INTEGER,
                title TEXT,
                published_at TEXT,
                updated_at TEXT,
                category TEXT,
                nominee TEXT,
                artist TEXT,
                workers TEXT,
                img TEXT,
                winner TEXT
            )
            """
        )
        connection.executemany(
            f"""
            INSERT INTO {settings.source_table_name} (
                year,
                title,
                published_at,
                updated_at,
                category,
                nominee,
                artist,
                workers,
                img,
                winner
            ) VALUES (
                :year,
                :title,
                :published_at,
                :updated_at,
                :category,
                :nominee,
                :artist,
                :workers,
                :img,
                :winner
            )
            """,
            raw_rows,
        )

    return settings.source_db_path


def extract_spotify_rows(settings: Settings) -> list[dict[str, str]]:
    return read_csv_rows(settings.spotify_csv_path)


def extract_grammy_rows(settings: Settings) -> list[dict[str, str]]:
    if not settings.source_db_path.exists():
        bootstrap_grammy_source_db(settings)

    with sqlite_connect(settings.source_db_path) as connection:
        cursor = connection.execute(
            f"""
            SELECT
                year,
                title,
                published_at,
                updated_at,
                category,
                nominee,
                artist,
                workers,
                img,
                winner
            FROM {settings.source_table_name}
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def stage_spotify_extraction(settings: Settings) -> Path:
    rows = extract_spotify_rows(settings)
    path = settings.staging_dir / "spotify_raw_extracted.csv"
    fieldnames = list(rows[0].keys()) if rows else []
    return write_csv_rows(path, rows, fieldnames=fieldnames)


def stage_grammy_extraction(settings: Settings) -> Path:
    rows = extract_grammy_rows(settings)
    path = settings.staging_dir / "grammy_raw_extracted.csv"
    return write_csv_rows(path, rows, fieldnames=GRAMMY_SOURCE_COLUMNS)

