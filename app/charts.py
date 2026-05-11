"""
charts.py — Plotly chart builders for the Streamlit dashboard.

Every function receives a DataFrame and returns a plotly Figure.
Keeping chart logic separate from UI logic (main.py) makes unit-testing trivial.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Colour palette (matches FMA project branding) ─────────────────────────────
_PALETTE = px.colors.sequential.Viridis


# ── Tempo tab ─────────────────────────────────────────────────────────────────

def tempo_bucket_bar(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar — track count per tempo bucket."""
    fig = px.bar(
        df,
        x="track_count",
        y="tempo_bucket",
        orientation="h",
        color="avg_tempo_bpm",
        color_continuous_scale=_PALETTE,
        text="track_count",
        labels={
            "track_count": "Tracks",
            "tempo_bucket": "Bucket",
            "avg_tempo_bpm": "BPM promedio",
        },
        title="Tracks por bucket de tempo",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=10, t=40, b=10))
    return fig


def tempo_stats_table(df: pd.DataFrame) -> go.Figure:
    """Table figure — min / avg / max / stddev BPM per bucket."""
    cols = ["tempo_bucket", "track_count", "min_tempo_bpm", "avg_tempo_bpm",
            "max_tempo_bpm", "stddev_tempo_bpm", "pct_of_total"]
    sub = df[cols].rename(columns={
        "tempo_bucket":    "Bucket",
        "track_count":     "Tracks",
        "min_tempo_bpm":   "Min BPM",
        "avg_tempo_bpm":   "Avg BPM",
        "max_tempo_bpm":   "Max BPM",
        "stddev_tempo_bpm": "Stddev",
        "pct_of_total":    "% total",
    })
    fig = go.Figure(
        go.Table(
            header=dict(values=list(sub.columns), fill_color="#1f2937", font_color="white"),
            cells=dict(values=[sub[c] for c in sub.columns], fill_color="#111827", font_color="#d1d5db"),
        )
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    return fig


# ── Features tab ──────────────────────────────────────────────────────────────

def feature_scatter(df: pd.DataFrame, x_col: str, y_col: str, color_col: str = "tempo_bucket") -> go.Figure:
    """Scatter plot — any two numeric feature columns."""
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        hover_data=["track_id", "title"],
        color_discrete_sequence=px.colors.qualitative.Plotly,
        title=f"{y_col} vs {x_col}",
    )
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return fig


def duration_histogram(df: pd.DataFrame) -> go.Figure:
    """Histogram of track duration in seconds."""
    fig = px.histogram(
        df,
        x="duration_sec",
        nbins=20,
        color="tempo_bucket",
        title="Distribución de duración",
        labels={"duration_sec": "Duración (seg)"},
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig.update_layout(bargap=0.1, margin=dict(l=10, r=10, t=40, b=10))
    return fig


def spectral_centroid_by_bucket(df: pd.DataFrame) -> go.Figure:
    """Box plot — spectral centroid distribution per tempo bucket."""
    fig = px.box(
        df,
        x="tempo_bucket",
        y="spectral_centroid_mean",
        color="tempo_bucket",
        points="all",
        title="Centroide espectral por bucket de tempo",
        labels={
            "tempo_bucket": "Bucket",
            "spectral_centroid_mean": "Centroide espectral (Hz)",
        },
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=40, b=10))
    return fig


# ── MFCC tab ──────────────────────────────────────────────────────────────────

def mfcc_bar(df: pd.DataFrame, track_id: str) -> go.Figure:
    """Bar chart — MFCC mean per coefficient for a single track."""
    row = df[df["track_id"] == track_id]
    if row.empty:
        return go.Figure()
    mfcc_mean = row["mfcc_mean"].iloc[0]
    # mfcc_mean is stored as a float (scalar mean across all coefficients & frames)
    # We show it as a single reference bar; if it's a list it would be richer.
    fig = go.Figure(
        go.Bar(x=["MFCC mean"], y=[float(mfcc_mean)], marker_color="#6366f1")
    )
    mfcc_std = row["mfcc_std"].iloc[0]
    fig.add_trace(
        go.Bar(x=["MFCC std"], y=[float(mfcc_std)], marker_color="#a5b4fc")
    )
    fig.update_layout(
        title=f"MFCC — track {track_id}",
        showlegend=False,
        margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig


def mfcc_heatmap(df: pd.DataFrame) -> go.Figure:
    """Heatmap — MFCC mean & std for all tracks."""
    sub = df[["track_id", "mfcc_mean", "mfcc_std"]].copy()
    sub = sub.set_index("track_id")
    fig = px.imshow(
        sub.T,
        color_continuous_scale="RdBu_r",
        title="MFCC mean / std por track",
        labels={"x": "Track ID", "y": "Métrica"},
    )
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return fig
