from __future__ import annotations

from pathlib import Path

from .config import Settings
from .utils import copy_file, ensure_directory, sqlite_connect


WAREHOUSE_TABLE_ORDER = [
    "dim_artist",
    "dim_genre",
    "dim_track",
    "dim_award",
    "fact_track_performance",
    "bridge_track_genre",
    "bridge_track_award",
]


def export_enriched_csv_to_local_drive(settings: Settings, source_path: Path) -> Path:
    target_path = settings.local_drive_export_dir / "spotify_grammy_enriched.csv"
    ensure_directory(settings.local_drive_export_dir)
    return copy_file(source_path, target_path)


def load_tables_into_warehouse(settings: Settings, tables: dict[str, list[dict]]) -> Path:
    schema_sql = settings.warehouse_schema_path.read_text(encoding="utf-8")
    ensure_directory(settings.warehouse_db_path.parent)

    with sqlite_connect(settings.warehouse_db_path) as connection:
        connection.executescript(schema_sql)

        for table_name in WAREHOUSE_TABLE_ORDER:
            rows = tables.get(table_name, [])
            if not rows:
                continue

            sanitized_rows = [
                {
                    column: (None if value == "" else value)
                    for column, value in row.items()
                }
                for row in rows
            ]
            columns = list(rows[0].keys())
            placeholders = ", ".join(f":{column}" for column in columns)
            quoted_columns = ", ".join(columns)

            connection.executemany(
                f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders})",
                sanitized_rows,
            )

    return settings.warehouse_db_path
