DROP VIEW IF EXISTS vw_track_grammy_matches;
DROP VIEW IF EXISTS vw_genre_popularity;
DROP VIEW IF EXISTS vw_top_artists;
DROP VIEW IF EXISTS vw_kpi_summary;

DROP TABLE IF EXISTS bridge_track_award;
DROP TABLE IF EXISTS bridge_track_genre;
DROP TABLE IF EXISTS fact_track_performance;
DROP TABLE IF EXISTS dim_award;
DROP TABLE IF EXISTS dim_track;
DROP TABLE IF EXISTS dim_genre;
DROP TABLE IF EXISTS dim_artist;

CREATE TABLE dim_artist (
    artist_key INTEGER PRIMARY KEY,
    artist_name TEXT NOT NULL,
    artist_name_normalized TEXT NOT NULL UNIQUE,
    grammy_win_count INTEGER NOT NULL,
    distinct_award_categories INTEGER NOT NULL,
    first_grammy_year INTEGER,
    last_grammy_year INTEGER,
    award_categories TEXT,
    award_years TEXT
);

CREATE TABLE dim_genre (
    genre_key INTEGER PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_track (
    track_key INTEGER PRIMARY KEY,
    track_id TEXT NOT NULL UNIQUE,
    track_name TEXT NOT NULL,
    album_name TEXT,
    primary_artist_name TEXT NOT NULL,
    explicit_flag INTEGER NOT NULL,
    genre_count INTEGER NOT NULL,
    all_genres TEXT,
    popularity_bucket TEXT NOT NULL,
    track_match_type TEXT NOT NULL
);

CREATE TABLE dim_award (
    award_key INTEGER PRIMARY KEY,
    award_year INTEGER,
    award_title TEXT,
    award_category TEXT,
    nominee TEXT,
    artist TEXT,
    winner_flag INTEGER NOT NULL
);

CREATE TABLE fact_track_performance (
    track_key INTEGER PRIMARY KEY,
    artist_key INTEGER,
    popularity INTEGER NOT NULL,
    popularity_min INTEGER NOT NULL,
    popularity_max INTEGER NOT NULL,
    popularity_conflict_flag INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    duration_minutes REAL NOT NULL,
    distinct_genre_count INTEGER NOT NULL,
    source_row_count INTEGER NOT NULL,
    danceability REAL NOT NULL,
    energy REAL NOT NULL,
    loudness REAL NOT NULL,
    speechiness REAL NOT NULL,
    acousticness REAL NOT NULL,
    instrumentalness REAL NOT NULL,
    liveness REAL NOT NULL,
    valence REAL NOT NULL,
    tempo REAL NOT NULL,
    time_signature INTEGER NOT NULL,
    has_track_grammy_match INTEGER NOT NULL,
    track_grammy_win_count INTEGER NOT NULL,
    artist_grammy_win_count INTEGER NOT NULL,
    energy_to_acoustic_ratio REAL NOT NULL,
    FOREIGN KEY (track_key) REFERENCES dim_track(track_key),
    FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key)
);

CREATE TABLE bridge_track_genre (
    track_key INTEGER NOT NULL,
    genre_key INTEGER NOT NULL,
    PRIMARY KEY (track_key, genre_key),
    FOREIGN KEY (track_key) REFERENCES dim_track(track_key),
    FOREIGN KEY (genre_key) REFERENCES dim_genre(genre_key)
);

CREATE TABLE bridge_track_award (
    track_key INTEGER NOT NULL,
    award_key INTEGER NOT NULL,
    match_type TEXT NOT NULL,
    PRIMARY KEY (track_key, award_key),
    FOREIGN KEY (track_key) REFERENCES dim_track(track_key),
    FOREIGN KEY (award_key) REFERENCES dim_award(award_key)
);

CREATE VIEW vw_kpi_summary AS
SELECT
    COUNT(*) AS total_tracks,
    SUM(has_track_grammy_match) AS matched_tracks,
    ROUND(AVG(popularity), 2) AS avg_popularity,
    ROUND(AVG(CASE WHEN has_track_grammy_match = 1 THEN popularity END), 2) AS avg_popularity_matched,
    ROUND(AVG(CASE WHEN has_track_grammy_match = 0 THEN popularity END), 2) AS avg_popularity_unmatched,
    SUM(popularity_conflict_flag) AS popularity_conflict_tracks,
    ROUND(AVG(energy), 4) AS avg_energy,
    ROUND(AVG(danceability), 4) AS avg_danceability
FROM fact_track_performance;

CREATE VIEW vw_top_artists AS
SELECT
    a.artist_name,
    a.grammy_win_count,
    COUNT(*) AS tracks_in_spotify,
    ROUND(AVG(f.popularity), 2) AS avg_popularity,
    ROUND(AVG(f.energy), 4) AS avg_energy
FROM fact_track_performance f
JOIN dim_artist a ON a.artist_key = f.artist_key
GROUP BY a.artist_key
ORDER BY a.grammy_win_count DESC, avg_popularity DESC;

CREATE VIEW vw_genre_popularity AS
SELECT
    g.genre_name,
    COUNT(*) AS track_count,
    ROUND(AVG(f.popularity), 2) AS avg_popularity,
    ROUND(AVG(f.danceability), 4) AS avg_danceability,
    ROUND(AVG(f.energy), 4) AS avg_energy,
    SUM(f.has_track_grammy_match) AS matched_tracks
FROM bridge_track_genre tg
JOIN dim_genre g ON g.genre_key = tg.genre_key
JOIN fact_track_performance f ON f.track_key = tg.track_key
GROUP BY g.genre_key
ORDER BY avg_popularity DESC;

CREATE VIEW vw_track_grammy_matches AS
SELECT
    t.track_name,
    t.primary_artist_name,
    t.all_genres,
    f.popularity,
    f.track_grammy_win_count,
    a.award_year,
    a.award_category,
    b.match_type
FROM bridge_track_award b
JOIN dim_track t ON t.track_key = b.track_key
JOIN fact_track_performance f ON f.track_key = t.track_key
JOIN dim_award a ON a.award_key = b.award_key;
