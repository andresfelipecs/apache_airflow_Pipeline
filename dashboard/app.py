from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "spotify_grammy_dw.sqlite"
DB_PATH = Path(os.getenv("WAREHOUSE_DB_PATH", DEFAULT_DB_PATH))


def query(sql: str) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as connection:
        return pd.read_sql_query(sql, connection)


st.set_page_config(page_title="Spotify + Grammy Dashboard", layout="wide")
st.title("Spotify + Grammy Analytics Dashboard")
st.caption("All metrics are queried from the SQLite data warehouse, never from the raw CSV files.")

if not DB_PATH.exists():
    st.error(f"The warehouse does not exist yet: {DB_PATH}")
    st.stop()

kpi = query("SELECT * FROM vw_kpi_summary")
top_artists = query(
    """
    SELECT artist_name, grammy_win_count, tracks_in_spotify, avg_popularity
    FROM vw_top_artists
    LIMIT 15
    """
)
genre_popularity = query(
    """
    SELECT genre_name, track_count, avg_popularity, matched_tracks
    FROM vw_genre_popularity
    LIMIT 15
    """
)
track_matches = query(
    """
    SELECT award_year, award_category, popularity, track_name, primary_artist_name, all_genres
    FROM vw_track_grammy_matches
    ORDER BY award_year DESC, popularity DESC
    LIMIT 50
    """
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Tracks in DW", int(kpi.loc[0, "total_tracks"]))
col2.metric("Exact Grammy matches", int(kpi.loc[0, "matched_tracks"]))
col3.metric("Avg popularity", float(kpi.loc[0, "avg_popularity"]))
col4.metric("Popularity conflict tracks", int(kpi.loc[0, "popularity_conflict_tracks"] or 0))

left, right = st.columns(2)

with left:
    st.subheader("Top artists by Grammy wins")
    fig = px.bar(
        top_artists,
        x="artist_name",
        y="grammy_win_count",
        color="avg_popularity",
        hover_data=["tracks_in_spotify", "avg_popularity"],
    )
    fig.update_layout(xaxis_title="", yaxis_title="Grammy wins")
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Genres by average popularity")
    fig = px.bar(
        genre_popularity,
        x="genre_name",
        y="avg_popularity",
        color="matched_tracks",
        hover_data=["track_count"],
    )
    fig.update_layout(xaxis_title="", yaxis_title="Average popularity")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Matched tracks by Grammy year")
if not track_matches.empty:
    year_chart = (
        track_matches.groupby("award_year", as_index=False)["track_name"]
        .count()
        .rename(columns={"track_name": "matched_tracks"})
    )
    fig = px.line(year_chart, x="award_year", y="matched_tracks", markers=True)
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Recent matched Grammy tracks")
st.dataframe(track_matches, use_container_width=True)
