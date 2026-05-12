"""
db.py — Database connection and query helpers for the Streamlit dashboard.

Uses @st.cache_resource for the SQLAlchemy engine (created once per process)
and @st.cache_data(ttl=300) for query results (refreshed every 5 minutes).
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ── Connection ─────────────────────────────────────────────────────────────────


@st.cache_resource(show_spinner="Conectando al warehouse…")
def get_engine() -> Engine:
    host = os.environ.get("WAREHOUSE_DB_HOST", "postgres-warehouse")
    db = os.environ.get("WAREHOUSE_DB_NAME", "music_features")
    user = os.environ.get("WAREHOUSE_DB_USER", "warehouse")
    pwd = os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}/{db}"
    return create_engine(url, pool_pre_ping=True)


# ── Query helpers ──────────────────────────────────────────────────────────────


@st.cache_data(ttl=300, show_spinner="Cargando estadísticas de tempo…")
def load_tempo_stats() -> pd.DataFrame:
    """mart_tempo_stats — one row per tempo bucket."""
    sql = """
        SELECT  tempo_bucket,
                track_count,
                avg_tempo_bpm,
                min_tempo_bpm,
                max_tempo_bpm,
                stddev_tempo_bpm,
                avg_spectral_centroid_hz,
                avg_duration_sec,
                pct_of_total
        FROM    marts.mart_tempo_stats
        ORDER BY avg_tempo_bpm
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


@st.cache_data(ttl=300, show_spinner="Cargando features de audio…")
def load_audio_features() -> pd.DataFrame:
    """fct_audio_features — one row per track with all numeric features."""
    sql = """
        SELECT  f.track_id,
                t.title,
                f.tempo,
                f.tempo_bucket,
                f.duration_sec,
                f.duration_bucket,
                f.spectral_centroid_mean,
                f.spectral_centroid_std,
                f.mfcc_mean,
                f.mfcc_std,
                f.sample_rate,
                f.processed_at
        FROM    marts.fct_audio_features  f
        LEFT JOIN marts.dim_tracks        t USING (track_id)
        ORDER BY f.processed_at DESC
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def load_pipeline_errors(limit: int = 100) -> pd.DataFrame:
    """pipeline_errors table — last N errors logged by the DAG.

    Returns an empty DataFrame (not cached) if the table doesn't exist yet,
    so the dashboard works before the first DAG run creates it.
    Note: intentionally NOT decorated with @st.cache_data so that exceptions
    are never cached — the caller handles the missing-table case gracefully.
    """
    sql = text(
        """
        SELECT  track_id, error_type, error_message, occurred_at
        FROM    public.pipeline_errors
        ORDER BY occurred_at DESC
        LIMIT   :lim
        """
    )
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(sql, conn, params={"lim": limit})
    except Exception:
        # Table doesn't exist yet (first run) or connection issue — return empty
        return pd.DataFrame(
            columns=["track_id", "error_type", "error_message", "occurred_at"]
        )


@st.cache_data(ttl=300, show_spinner="Cargando KPIs…")
def load_kpis() -> dict:
    """High-level numbers shown in the Overview tab."""
    sql = """
        SELECT
            COUNT(*)                            AS total_tracks,
            ROUND(AVG(tempo)::numeric, 1)       AS avg_bpm,
            ROUND(AVG(duration_sec)::numeric, 1) AS avg_duration_sec,
            MAX(processed_at)                   AS last_run
        FROM marts.fct_audio_features
    """
    with get_engine().connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    if row is None:
        return {
            "total_tracks": 0,
            "avg_bpm": 0,
            "avg_duration_sec": 0,
            "last_run": None,
        }
    return {
        "total_tracks": int(row[0]),
        "avg_bpm": float(row[1] or 0),
        "avg_duration_sec": float(row[2] or 0),
        "last_run": row[3],
    }
