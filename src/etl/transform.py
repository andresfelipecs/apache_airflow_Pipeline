from __future__ import annotations

from collections import defaultdict

from .utils import unique_join


def _assign_award_keys(grammy_rows: list[dict]) -> list[dict]:
    award_rows: list[dict] = []

    for award_key, row in enumerate(grammy_rows, start=1):
        award = dict(row)
        award["award_key"] = award_key
        award_rows.append(award)

    return award_rows


def _build_artist_award_stats(award_rows: list[dict]) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    grouped: dict[str, list[dict]] = defaultdict(list)

    for award in award_rows:
        artist_key = award["artist_normalized"]
        if artist_key:
            grouped[artist_key].append(award)

    for artist_key, awards in grouped.items():
        years = sorted({award["year"] for award in awards if award["year"]})
        categories = sorted({award["category"] for award in awards if award["category"]})
        stats[artist_key] = {
            "artist_grammy_win_count": len(awards),
            "artist_distinct_award_categories": len(categories),
            "artist_first_grammy_year": years[0] if years else None,
            "artist_last_grammy_year": years[-1] if years else None,
            "artist_award_categories": unique_join(categories),
            "artist_award_years": unique_join([str(year) for year in years]),
        }

    return stats


def _build_track_award_lookup(award_rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    lookup: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for award in award_rows:
        artist_key = award["artist_normalized"]
        nominee_key = award["nominee_normalized"]
        if artist_key and nominee_key:
            lookup[(artist_key, nominee_key)].append(award)

    return dict(lookup)


def enrich_spotify_with_grammys(
    spotify_rows: list[dict], grammy_rows: list[dict]
) -> tuple[list[dict], dict[str, list[dict]], dict]:
    award_rows = _assign_award_keys(grammy_rows)
    artist_stats_lookup = _build_artist_award_stats(award_rows)
    track_award_lookup = _build_track_award_lookup(award_rows)

    enriched_rows: list[dict] = []
    bridge_rows: list[dict] = []
    exact_track_matches = 0
    artist_only_matches = 0

    for spotify_row in spotify_rows:
        track_awards = track_award_lookup.get(
            (
                spotify_row["primary_artist_normalized"],
                spotify_row["track_name_normalized"],
            ),
            [],
        )
        artist_stats = artist_stats_lookup.get(
            spotify_row["primary_artist_normalized"],
            {
                "artist_grammy_win_count": 0,
                "artist_distinct_award_categories": 0,
                "artist_first_grammy_year": None,
                "artist_last_grammy_year": None,
                "artist_award_categories": "",
                "artist_award_years": "",
            },
        )

        matched_categories = [award["category"] for award in track_awards]
        matched_years = sorted({award["year"] for award in track_awards if award["year"]})
        track_match_type = "no_match"
        if track_awards:
            track_match_type = "track_and_artist"
            exact_track_matches += 1
        elif artist_stats["artist_grammy_win_count"] > 0:
            track_match_type = "artist_only"
            artist_only_matches += 1

        enriched = {
            **spotify_row,
            **artist_stats,
            "has_track_grammy_match": 1 if track_awards else 0,
            "track_grammy_win_count": len(track_awards),
            "track_distinct_award_categories": len(set(matched_categories)),
            "track_grammy_categories": unique_join(matched_categories),
            "track_grammy_years": unique_join([str(year) for year in matched_years]),
            "track_match_type": track_match_type,
        }
        enriched_rows.append(enriched)

        for award in track_awards:
            bridge_rows.append(
                {
                    "track_id": spotify_row["track_id"],
                    "award_key": award["award_key"],
                    "match_type": "track_and_artist",
                }
            )

    tables = build_star_schema(enriched_rows, award_rows, bridge_rows)
    metrics = {
        "spotify_tracks_after_cleaning": len(spotify_rows),
        "grammy_awards_after_cleaning": len(grammy_rows),
        "multi_genre_tracks_after_cleaning": sum(
            1 for row in spotify_rows if int(row.get("genre_count", 0)) > 1
        ),
        "popularity_conflict_tracks_after_cleaning": sum(
            1 for row in spotify_rows if int(row.get("popularity_conflict_flag", 0)) == 1
        ),
        "exact_track_matches": exact_track_matches,
        "artist_only_matches": artist_only_matches,
        "unmatched_tracks": len(spotify_rows) - exact_track_matches - artist_only_matches,
    }

    return enriched_rows, tables, metrics


def build_star_schema(
    enriched_rows: list[dict], award_rows: list[dict], bridge_rows: list[dict]
) -> dict[str, list[dict]]:
    dim_artist: list[dict] = []
    dim_genre: list[dict] = []
    dim_track: list[dict] = []
    dim_award: list[dict] = []
    fact_track_performance: list[dict] = []
    bridge_track_award: list[dict] = []
    bridge_track_genre: list[dict] = []

    artist_key_lookup: dict[str, int] = {}
    genre_key_lookup: dict[str, int] = {}
    track_key_lookup: dict[str, int] = {}

    unique_artists = sorted(
        {row["primary_artist_normalized"]: row for row in enriched_rows if row["primary_artist_normalized"]}.values(),
        key=lambda row: row["primary_artist_normalized"],
    )
    unique_genres = sorted(
        {
            genre
            for row in enriched_rows
            for genre in row.get("track_genres", "").split(" | ")
            if genre
        }
    )
    unique_tracks = sorted(enriched_rows, key=lambda row: row["track_id"])

    for artist_key, row in enumerate(unique_artists, start=1):
        artist_key_lookup[row["primary_artist_normalized"]] = artist_key
        dim_artist.append(
            {
                "artist_key": artist_key,
                "artist_name": row["primary_artist"],
                "artist_name_normalized": row["primary_artist_normalized"],
                "grammy_win_count": row["artist_grammy_win_count"],
                "distinct_award_categories": row["artist_distinct_award_categories"],
                "first_grammy_year": row["artist_first_grammy_year"],
                "last_grammy_year": row["artist_last_grammy_year"],
                "award_categories": row["artist_award_categories"],
                "award_years": row["artist_award_years"],
            }
        )

    for genre_key, genre_name in enumerate(unique_genres, start=1):
        genre_key_lookup[genre_name] = genre_key
        dim_genre.append({"genre_key": genre_key, "genre_name": genre_name})

    for track_key, row in enumerate(unique_tracks, start=1):
        track_key_lookup[row["track_id"]] = track_key
        dim_track.append(
            {
                "track_key": track_key,
                "track_id": row["track_id"],
                "track_name": row["track_name"],
                "album_name": row["album_name"],
                "primary_artist_name": row["primary_artist"],
                "explicit_flag": row["explicit"],
                "genre_count": row["genre_count"],
                "all_genres": row["track_genres"],
                "popularity_bucket": row["popularity_bucket"],
                "track_match_type": row["track_match_type"],
            }
        )

    for award in award_rows:
        dim_award.append(
            {
                "award_key": award["award_key"],
                "award_year": award["year"],
                "award_title": award["title"],
                "award_category": award["category"],
                "nominee": award["nominee"],
                "artist": award["artist"],
                "winner_flag": award["winner"],
            }
        )

    for row in unique_tracks:
        artist_key = artist_key_lookup.get(row["primary_artist_normalized"])
        fact_track_performance.append(
            {
                "track_key": track_key_lookup[row["track_id"]],
                "artist_key": artist_key,
                "popularity": row["popularity"],
                "popularity_min": row["popularity_min"],
                "popularity_max": row["popularity_max"],
                "popularity_conflict_flag": row["popularity_conflict_flag"],
                "duration_ms": row["duration_ms"],
                "duration_minutes": row["duration_minutes"],
                "distinct_genre_count": row["genre_count"],
                "source_row_count": row["source_row_count"],
                "danceability": row["danceability"],
                "energy": row["energy"],
                "loudness": row["loudness"],
                "speechiness": row["speechiness"],
                "acousticness": row["acousticness"],
                "instrumentalness": row["instrumentalness"],
                "liveness": row["liveness"],
                "valence": row["valence"],
                "tempo": row["tempo"],
                "time_signature": row["time_signature"],
                "has_track_grammy_match": row["has_track_grammy_match"],
                "track_grammy_win_count": row["track_grammy_win_count"],
                "artist_grammy_win_count": row["artist_grammy_win_count"],
                "energy_to_acoustic_ratio": row["energy_to_acoustic_ratio"],
            }
        )

        for genre in row.get("track_genres", "").split(" | "):
            if genre:
                bridge_track_genre.append(
                    {
                        "track_key": track_key_lookup[row["track_id"]],
                        "genre_key": genre_key_lookup[genre],
                    }
                )

    for bridge in bridge_rows:
        bridge_track_award.append(
            {
                "track_key": track_key_lookup[bridge["track_id"]],
                "award_key": bridge["award_key"],
                "match_type": bridge["match_type"],
            }
        )

    return {
        "dim_artist": dim_artist,
        "dim_genre": dim_genre,
        "dim_track": dim_track,
        "dim_award": dim_award,
        "fact_track_performance": fact_track_performance,
        "bridge_track_genre": bridge_track_genre,
        "bridge_track_award": bridge_track_award,
    }
