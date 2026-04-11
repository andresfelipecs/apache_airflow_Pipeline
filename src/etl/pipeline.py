from __future__ import annotations

from pathlib import Path

from .clean import clean_grammy_rows, clean_spotify_rows
from .config import Settings, get_settings
from .extract import (
    bootstrap_grammy_source_db,
    stage_grammy_extraction,
    stage_spotify_extraction,
)
from .load import export_enriched_csv_to_local_drive, load_tables_into_warehouse
from .transform import enrich_spotify_with_grammys
from .utils import read_csv_rows, write_csv_rows, write_json


STAGE_FILES = {
    "spotify_raw": "spotify_raw_extracted.csv",
    "grammy_raw": "grammy_raw_extracted.csv",
    "spotify_clean": "spotify_clean.csv",
    "grammy_clean": "grammy_clean.csv",
    "enriched": "spotify_grammy_enriched.csv",
    "dim_artist": "dim_artist.csv",
    "dim_genre": "dim_genre.csv",
    "dim_track": "dim_track.csv",
    "dim_award": "dim_award.csv",
    "fact_track_performance": "fact_track_performance.csv",
    "bridge_track_genre": "bridge_track_genre.csv",
    "bridge_track_award": "bridge_track_award.csv",
}


def _stage_path(settings: Settings, key: str) -> Path:
    return settings.staging_dir / STAGE_FILES[key]


def get_runtime_settings(settings: Settings | None = None) -> Settings:
    return settings or get_settings()


def bootstrap_source_database(settings: Settings | None = None) -> str:
    runtime_settings = get_runtime_settings(settings)
    path = bootstrap_grammy_source_db(runtime_settings)
    return str(path)


def extract_spotify_task(settings: Settings | None = None) -> str:
    runtime_settings = get_runtime_settings(settings)
    path = stage_spotify_extraction(runtime_settings)
    return str(path)


def extract_grammy_task(settings: Settings | None = None) -> str:
    runtime_settings = get_runtime_settings(settings)
    path = stage_grammy_extraction(runtime_settings)
    return str(path)


def clean_datasets_task(settings: Settings | None = None) -> dict:
    runtime_settings = get_runtime_settings(settings)

    spotify_rows = read_csv_rows(_stage_path(runtime_settings, "spotify_raw"))
    grammy_rows = read_csv_rows(_stage_path(runtime_settings, "grammy_raw"))

    spotify_clean_rows, spotify_metrics = clean_spotify_rows(spotify_rows)
    grammy_clean_rows, grammy_metrics = clean_grammy_rows(grammy_rows)

    write_csv_rows(_stage_path(runtime_settings, "spotify_clean"), spotify_clean_rows)
    write_csv_rows(_stage_path(runtime_settings, "grammy_clean"), grammy_clean_rows)

    metrics = {
        "spotify": spotify_metrics,
        "grammy": grammy_metrics,
    }
    write_json(runtime_settings.reports_dir / "cleaning_summary.json", metrics)
    return metrics


def transform_and_merge_task(settings: Settings | None = None) -> dict:
    runtime_settings = get_runtime_settings(settings)

    spotify_clean_rows = read_csv_rows(_stage_path(runtime_settings, "spotify_clean"))
    grammy_clean_rows = read_csv_rows(_stage_path(runtime_settings, "grammy_clean"))

    enriched_rows, tables, metrics = enrich_spotify_with_grammys(
        spotify_clean_rows,
        grammy_clean_rows,
    )

    write_csv_rows(_stage_path(runtime_settings, "enriched"), enriched_rows)

    for table_name, rows in tables.items():
        write_csv_rows(_stage_path(runtime_settings, table_name), rows)

    write_json(runtime_settings.reports_dir / "merge_summary.json", metrics)
    return metrics


def export_csv_to_local_drive_task(settings: Settings | None = None) -> str:
    runtime_settings = get_runtime_settings(settings)
    export_path = export_enriched_csv_to_local_drive(
        runtime_settings,
        _stage_path(runtime_settings, "enriched"),
    )
    return str(export_path)


def load_data_warehouse_task(settings: Settings | None = None) -> str:
    runtime_settings = get_runtime_settings(settings)
    tables = {
        table_name: read_csv_rows(_stage_path(runtime_settings, table_name))
        for table_name in (
            "dim_artist",
            "dim_genre",
            "dim_track",
            "dim_award",
            "fact_track_performance",
            "bridge_track_genre",
            "bridge_track_award",
        )
    }
    warehouse_path = load_tables_into_warehouse(runtime_settings, tables)
    return str(warehouse_path)


def run_pipeline(settings: Settings | None = None) -> dict:
    runtime_settings = get_runtime_settings(settings)

    bootstrap_source_database(runtime_settings)
    extract_spotify_task(runtime_settings)
    extract_grammy_task(runtime_settings)
    cleaning_metrics = clean_datasets_task(runtime_settings)
    merge_metrics = transform_and_merge_task(runtime_settings)
    export_path = export_csv_to_local_drive_task(runtime_settings)
    warehouse_path = load_data_warehouse_task(runtime_settings)

    summary = {
        "cleaning_metrics": cleaning_metrics,
        "merge_metrics": merge_metrics,
        "csv_export_path": export_path,
        "warehouse_path": warehouse_path,
    }
    write_json(runtime_settings.reports_dir / "pipeline_run_summary.json", summary)
    return summary
