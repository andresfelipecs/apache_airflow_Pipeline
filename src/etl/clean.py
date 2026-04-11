from __future__ import annotations

from collections import Counter, defaultdict

from .utils import (
    normalize_text,
    parse_bool,
    parse_float,
    parse_int,
    popularity_bucket,
    split_primary_artist,
)


SPOTIFY_NUMERIC_FIELDS = {
    "popularity": parse_int,
    "duration_ms": parse_int,
    "danceability": parse_float,
    "energy": parse_float,
    "key": parse_int,
    "loudness": parse_float,
    "mode": parse_int,
    "speechiness": parse_float,
    "acousticness": parse_float,
    "instrumentalness": parse_float,
    "liveness": parse_float,
    "valence": parse_float,
    "tempo": parse_float,
    "time_signature": parse_int,
}


def _pick_canonical_text(values: list[str]) -> str:
    cleaned_values = [value for value in values if value]
    if not cleaned_values:
        return ""

    counts = Counter(cleaned_values)
    return max(counts.items(), key=lambda item: (item[1], len(item[0]), item[0]))[0]


def _pick_canonical_numeric(values: list[int | float]) -> int | float:
    counts = Counter(values)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def clean_spotify_rows(rows: list[dict[str, str]]) -> tuple[list[dict], dict]:
    grouped_tracks: dict[str, list[dict]] = defaultdict(list)
    invalid_rows = 0
    duplicate_profile_rows_removed = 0
    multi_genre_track_ids = 0
    popularity_conflict_track_ids = 0
    artist_track_album_conflict_track_ids = 0
    duplicate_track_id_groups = 0

    for raw_row in rows:
        row = {
            str(key).strip(): ("" if value is None else str(value).strip())
            for key, value in raw_row.items()
        }
        source_row_id = row.get("", row.get("source_row_id", ""))
        track_id = row.get("track_id", "")
        track_name = row.get("track_name", "")

        if not track_id or not track_name:
            invalid_rows += 1
            continue

        cleaned = {
            "source_row_id": source_row_id,
            "track_id": track_id,
            "artists": row.get("artists", ""),
            "album_name": row.get("album_name", ""),
            "track_name": track_name,
            "explicit": parse_bool(row.get("explicit")),
            "track_genre": row.get("track_genre", "").lower(),
        }

        for field_name, parser in SPOTIFY_NUMERIC_FIELDS.items():
            cleaned[field_name] = parser(row.get(field_name))

        primary_artist = split_primary_artist(cleaned["artists"]) or cleaned["artists"] or "Unknown Artist"
        cleaned["primary_artist"] = primary_artist
        cleaned["primary_artist_normalized"] = normalize_text(primary_artist)
        cleaned["track_name_normalized"] = normalize_text(cleaned["track_name"])
        cleaned["album_name_normalized"] = normalize_text(cleaned["album_name"])
        cleaned["duration_minutes"] = round(cleaned["duration_ms"] / 60000, 2)
        cleaned["popularity_bucket"] = popularity_bucket(cleaned["popularity"])

        acousticness = cleaned["acousticness"] or 0.0001
        cleaned["energy_to_acoustic_ratio"] = round(cleaned["energy"] / acousticness, 4)

        grouped_tracks[track_id].append(cleaned)

    canonical_rows: list[dict] = []

    for track_rows in grouped_tracks.values():
        if len(track_rows) > 1:
            duplicate_track_id_groups += 1

        row_profiles = {
            tuple(
                row[column]
                for column in (
                    "track_id",
                    "artists",
                    "album_name",
                    "track_name",
                    "explicit",
                    "track_genre",
                    "popularity",
                    "duration_ms",
                    "danceability",
                    "energy",
                    "key",
                    "loudness",
                    "mode",
                    "speechiness",
                    "acousticness",
                    "instrumentalness",
                    "liveness",
                    "valence",
                    "tempo",
                    "time_signature",
                )
            )
            for row in track_rows
        }
        duplicate_profile_rows_removed += len(track_rows) - len(row_profiles)

        unique_genres = sorted({row["track_genre"] for row in track_rows if row["track_genre"]})
        popularity_values = sorted({row["popularity"] for row in track_rows})
        if len(unique_genres) > 1:
            multi_genre_track_ids += 1
        if len(popularity_values) > 1:
            popularity_conflict_track_ids += 1

        distinct_artists = {row["artists"] for row in track_rows if row["artists"]}
        distinct_track_names = {row["track_name"] for row in track_rows if row["track_name"]}
        distinct_album_names = {row["album_name"] for row in track_rows if row["album_name"]}
        if (
            len(distinct_artists) > 1
            or len(distinct_track_names) > 1
            or len(distinct_album_names) > 1
        ):
            artist_track_album_conflict_track_ids += 1

        canonical_popularity = _pick_canonical_numeric([row["popularity"] for row in track_rows])
        popularity_selection_strategy = (
            "consistent"
            if len(popularity_values) == 1
            else "mode_then_max_tiebreak"
        )
        selected_row = max(
            track_rows,
            key=lambda row: (
                row["popularity"] == canonical_popularity,
                row["track_genre"] != "",
                row["duration_ms"],
                row["track_genre"],
            ),
        )

        canonical = dict(selected_row)
        canonical["artists"] = _pick_canonical_text([row["artists"] for row in track_rows])
        canonical["album_name"] = _pick_canonical_text([row["album_name"] for row in track_rows])
        canonical["track_name"] = _pick_canonical_text([row["track_name"] for row in track_rows])
        canonical["primary_artist"] = _pick_canonical_text([row["primary_artist"] for row in track_rows])
        canonical["primary_artist_normalized"] = normalize_text(canonical["primary_artist"])
        canonical["track_name_normalized"] = normalize_text(canonical["track_name"])
        canonical["album_name_normalized"] = normalize_text(canonical["album_name"])
        canonical["track_genres"] = " | ".join(unique_genres)
        canonical["genre_count"] = len(unique_genres)
        canonical["source_row_count"] = len(track_rows)
        canonical["popularity"] = canonical_popularity
        canonical["popularity_min"] = min(popularity_values) if popularity_values else canonical_popularity
        canonical["popularity_max"] = max(popularity_values) if popularity_values else canonical_popularity
        canonical["popularity_conflict_flag"] = 1 if len(popularity_values) > 1 else 0
        canonical["popularity_values_observed"] = " | ".join(str(value) for value in popularity_values)
        canonical["popularity_selection_strategy"] = popularity_selection_strategy

        acousticness = canonical["acousticness"] or 0.0001
        canonical["energy_to_acoustic_ratio"] = round(canonical["energy"] / acousticness, 4)
        canonical["popularity_bucket"] = popularity_bucket(canonical["popularity"])

        canonical_rows.append(canonical)

    canonical_rows.sort(key=lambda row: (row["primary_artist"], row["track_name"]))

    metrics = {
        "input_rows": len(rows),
        "invalid_rows_removed": invalid_rows,
        "duplicate_track_id_groups": duplicate_track_id_groups,
        "track_id_groups_collapsed": len(rows) - invalid_rows - len(canonical_rows),
        "duplicate_profile_rows_removed": duplicate_profile_rows_removed,
        "multi_genre_track_ids": multi_genre_track_ids,
        "popularity_conflict_track_ids": popularity_conflict_track_ids,
        "artist_track_album_conflict_track_ids": artist_track_album_conflict_track_ids,
        "output_rows": len(canonical_rows),
    }

    return canonical_rows, metrics


def clean_grammy_rows(rows: list[dict[str, str]]) -> tuple[list[dict], dict]:
    cleaned_rows: list[dict] = []
    rows_missing_artist = 0

    for raw_row in rows:
        row = {
            str(key).strip(): ("" if value is None else str(value).strip())
            for key, value in raw_row.items()
        }
        artist = row.get("artist", "")
        nominee = row.get("nominee", "")

        if not artist:
            rows_missing_artist += 1

        cleaned = {
            "year": parse_int(row.get("year"), default=0),
            "title": row.get("title", ""),
            "published_at": row.get("published_at", ""),
            "updated_at": row.get("updated_at", ""),
            "category": row.get("category", ""),
            "nominee": nominee,
            "artist": artist,
            "workers": row.get("workers", ""),
            "img": row.get("img", ""),
            "winner": parse_bool(row.get("winner")),
            "artist_normalized": normalize_text(artist),
            "nominee_normalized": normalize_text(nominee),
            "category_normalized": normalize_text(row.get("category", "")),
        }
        cleaned_rows.append(cleaned)

    cleaned_rows.sort(key=lambda row: (row["year"], row["category"], row["artist"], row["nominee"]))

    metrics = {
        "input_rows": len(rows),
        "rows_missing_artist": rows_missing_artist,
        "output_rows": len(cleaned_rows),
    }

    return cleaned_rows, metrics
