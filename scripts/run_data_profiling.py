from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.etl.clean import clean_grammy_rows, clean_spotify_rows
from src.etl.config import get_settings
from src.etl.extract import extract_grammy_rows, extract_spotify_rows
from src.etl.transform import enrich_spotify_with_grammys


def build_markdown_report() -> str:
    settings = get_settings()
    spotify_raw = extract_spotify_rows(settings)
    grammy_raw = extract_grammy_rows(settings)

    spotify_clean, spotify_metrics = clean_spotify_rows(spotify_raw)
    grammy_clean, grammy_metrics = clean_grammy_rows(grammy_raw)
    _, _, merge_metrics = enrich_spotify_with_grammys(spotify_clean, grammy_clean)

    genre_counter = Counter()
    for row in spotify_clean:
        for genre in row.get("track_genres", "").split(" | "):
            if genre:
                genre_counter[genre] += 1
    artist_counter = Counter(row["primary_artist"] for row in spotify_clean)
    award_artist_counter = Counter(row["artist"] for row in grammy_clean if row["artist"])
    category_counter = Counter(row["category"] for row in grammy_clean if row["category"])

    lines = [
        "# Data Profiling Report",
        "",
        "## Data quality findings",
        f"- Duplicated `track_id` groups in Spotify: {spotify_metrics['duplicate_track_id_groups']}",
        f"- Extra Spotify rows collapsed into canonical tracks: {spotify_metrics['track_id_groups_collapsed']}",
        f"- Spotify tracks with multiple genres: {spotify_metrics['multi_genre_track_ids']}",
        f"- Spotify tracks with popularity conflicts across duplicate rows: {spotify_metrics['popularity_conflict_track_ids']}",
        f"- Spotify duplicate profile rows removed as exact duplicates: {spotify_metrics['duplicate_profile_rows_removed']}",
        f"- Spotify duplicate groups with artist/track/album conflicts: {spotify_metrics['artist_track_album_conflict_track_ids']}",
        "- Main finding: duplicated Spotify `track_id` values do not represent different songs; they overwhelmingly represent the same track repeated across multiple genres.",
        "- Modeling decision: keep one canonical row per `track_id` in the fact table and preserve all genres in a dedicated bridge table instead of discarding genres.",
        "",
        "## Spotify dataset",
        f"- Raw rows: {len(spotify_raw)}",
        f"- Clean rows: {spotify_metrics['output_rows']}",
        f"- Invalid rows removed: {spotify_metrics['invalid_rows_removed']}",
        f"- Track ID groups collapsed into canonical tracks: {spotify_metrics['track_id_groups_collapsed']}",
        f"- Unique genres: {len(genre_counter)}",
        "",
        "Top Spotify genres:",
    ]

    for genre, count in genre_counter.most_common(10):
        lines.append(f"- {genre}: {count}")

    lines.extend(
        [
            "",
            "Top Spotify artists:",
        ]
    )

    for artist, count in artist_counter.most_common(10):
        lines.append(f"- {artist}: {count}")

    lines.extend(
        [
            "",
            "## Grammy dataset",
            f"- Raw rows: {len(grammy_raw)}",
            f"- Clean rows: {grammy_metrics['output_rows']}",
            f"- Rows missing artist: {grammy_metrics['rows_missing_artist']}",
            f"- Distinct award categories: {len(category_counter)}",
            "",
            "Top Grammy categories:",
        ]
    )

    for category, count in category_counter.most_common(10):
        lines.append(f"- {category}: {count}")

    lines.extend(
        [
            "",
            "Top Grammy artists:",
        ]
    )

    for artist, count in award_artist_counter.most_common(10):
        lines.append(f"- {artist}: {count}")

    lines.extend(
        [
            "",
            "## Cross-dataset matching",
            f"- Exact track + artist matches: {merge_metrics['exact_track_matches']}",
            f"- Artist-only matches: {merge_metrics['artist_only_matches']}",
            f"- Unmatched Spotify tracks: {merge_metrics['unmatched_tracks']}",
            "",
            "## Notes",
            "- The Spotify dataset contains a blank leading index column that is removed during cleaning.",
            "- The Grammy dataset behaves like a winners-only history because every observed value in `winner` is `True`.",
            "- Spotify duplicate `track_id` values are preserved analytically through a track-to-genre bridge instead of forcing a single genre per track.",
            "- Due to the nature of the sources, the strongest deterministic merge is exact normalized match on primary artist + track/nominee name, with artist-level wins used as enrichment fallback.",
        ]
    )

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    settings = get_settings()
    report = build_markdown_report()
    output_path = settings.reports_dir / "data_profiling.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(f"Profiling report written to: {output_path}")
