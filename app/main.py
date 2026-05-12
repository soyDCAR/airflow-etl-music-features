"""
main.py — Streamlit dashboard for the FMA audio features pipeline.

Layout: 5 tabs
  1. Overview   — KPI cards + recent activity
  2. Tempo      — bucket distribution, stats table
  3. Features   — scatter / histogram / box plots
  4. MFCC       — per-track MFCC viewer + heatmap
  5. Data       — raw table + CSV download
"""
from __future__ import annotations

import streamlit as st

from charts import (
    duration_histogram,
    feature_scatter,
    mfcc_bar,
    mfcc_heatmap,
    spectral_centroid_by_bucket,
    tempo_bucket_bar,
    tempo_stats_table,
)
from db import load_audio_features, load_kpis, load_pipeline_errors, load_tempo_stats

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FMA Audio Features",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("🎵 FMA Audio Features Pipeline")
st.caption(
    "Airflow 2.9 · librosa · dbt · Postgres 15 · Streamlit  —  "
    "datos actualizados cada **5 minutos**"
)
st.divider()

# ── Load data (cached) ─────────────────────────────────────────────────────────
with st.spinner("Cargando datos…"):
    kpis        = load_kpis()
    tempo_stats = load_tempo_stats()
    audio_df    = load_audio_features()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_overview, tab_tempo, tab_features, tab_mfcc, tab_data = st.tabs(
    ["📊 Overview", "🥁 Tempo", "📈 Features", "🎼 MFCC", "📋 Data"]
)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Overview
# ═══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🎵 Tracks procesados", f"{kpis['total_tracks']:,}")
    c2.metric("🥁 BPM promedio", f"{kpis['avg_bpm']:.1f}")
    c3.metric("⏱ Duración promedio", f"{kpis['avg_duration_sec']:.0f} s")
    last_run = kpis["last_run"]
    c4.metric("🕐 Último ingreso", str(last_run)[:19] if last_run else "—")

    st.subheader("Distribución por bucket de tempo")
    if not tempo_stats.empty:
        st.plotly_chart(tempo_bucket_bar(tempo_stats), use_container_width=True)
    else:
        st.info("Sin datos todavía. Ejecuta el DAG en Airflow primero.")

    st.subheader("Últimos errores del pipeline")
    errors_df = load_pipeline_errors(limit=10)
    if errors_df.empty:
        st.success("✅ Sin errores registrados.")
    else:
        st.dataframe(errors_df, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Tempo
# ═══════════════════════════════════════════════════════════════════════════════
with tab_tempo:
    if tempo_stats.empty:
        st.info("Sin datos de tempo. Ejecuta el DAG primero.")
    else:
        col_left, col_right = st.columns([3, 2])
        with col_left:
            st.plotly_chart(tempo_bucket_bar(tempo_stats), use_container_width=True)
        with col_right:
            st.plotly_chart(spectral_centroid_by_bucket(audio_df), use_container_width=True)

        st.subheader("Estadísticas por bucket")
        st.plotly_chart(tempo_stats_table(tempo_stats), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Features
# ═══════════════════════════════════════════════════════════════════════════════
with tab_features:
    if audio_df.empty:
        st.info("Sin datos de audio features.")
    else:
        st.subheader("Scatter plot interactivo")
        numeric_cols = ["tempo", "duration_sec", "spectral_centroid_mean", "spectral_centroid_std",
                        "mfcc_mean", "mfcc_std"]
        col_x, col_y = st.columns(2)
        x_col = col_x.selectbox("Eje X", numeric_cols, index=0)
        y_col = col_y.selectbox("Eje Y", numeric_cols, index=2)
        st.plotly_chart(feature_scatter(audio_df, x_col, y_col), use_container_width=True)

        st.subheader("Distribución de duración")
        st.plotly_chart(duration_histogram(audio_df), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — MFCC
# ═══════════════════════════════════════════════════════════════════════════════
with tab_mfcc:
    if audio_df.empty:
        st.info("Sin datos de MFCC.")
    else:
        st.subheader("MFCC por track")
        track_ids = audio_df["track_id"].tolist()
        selected  = st.selectbox("Selecciona un track", track_ids)
        if selected:
            row = audio_df[audio_df["track_id"] == selected].iloc[0]
            title = row.get("title", "—") or "—"
            st.caption(f"**{title}** · BPM: {row['tempo']:.1f} · Bucket: {row['tempo_bucket']}")
            st.plotly_chart(mfcc_bar(audio_df, selected), use_container_width=True)

        st.subheader("Heatmap global de MFCC")
        st.plotly_chart(mfcc_heatmap(audio_df), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Data
# ═══════════════════════════════════════════════════════════════════════════════
with tab_data:
    st.subheader("Tabla completa — fct_audio_features")
    if audio_df.empty:
        st.info("Sin datos.")
    else:
        st.dataframe(audio_df, use_container_width=True)
        csv = audio_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Descargar CSV",
            data=csv,
            file_name="fma_audio_features.csv",
            mime="text/csv",
        )

    st.subheader("Estadísticas de tempo — mart_tempo_stats")
    if not tempo_stats.empty:
        st.dataframe(tempo_stats, use_container_width=True)
