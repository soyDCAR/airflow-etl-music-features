"""
DAG: extract_fma_features

Ingests Free Music Archive (FMA) audio samples, extracts acoustic features
with librosa, and loads them into the postgres-warehouse `audio_features` table.

Airflow Variables (set via UI or `airflow variables set`):
  FMA_SAMPLE_SIZE     Number of tracks to process       (default: 100)
  FMA_DATA_DIR        Container path for audio files    (default: /opt/airflow/data/fma)
  FMA_USE_SYNTHETIC   "true" → synthetic signals, no download (default: true)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="extract_fma_features",
    description="FMA → librosa features → Postgres warehouse",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fma", "audio-features", "etl", "librosa"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
)
def extract_fma_features() -> None:

    @task(task_id="download_fma_sample")
    def download_fma_sample() -> list[dict[str, Any]]:
        """Stage audio tracks for feature extraction.

        FMA_USE_SYNTHETIC=true  → generate synthetic sine-wave signals (default, no I/O).
        FMA_USE_SYNTHETIC=false → download real MP3s from the FMA archive (~7 GB for
                                  fma_small). Set FMA_DATA_DIR to a volume with space.
        """
        use_synthetic = Variable.get("FMA_USE_SYNTHETIC", default_var="true").lower() == "true"
        sample_size = int(Variable.get("FMA_SAMPLE_SIZE", default_var="100"))
        data_dir = Path(Variable.get("FMA_DATA_DIR", default_var="/opt/airflow/data/fma"))
        data_dir.mkdir(parents=True, exist_ok=True)

        if use_synthetic:
            return _generate_synthetic_tracks(sample_size, data_dir)
        return _download_fma_tracks(sample_size, data_dir)

    @task(task_id="extract_features")
    def extract_features(tracks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract MFCC (13 coefficients), spectral centroid, and tempo per track."""
        import librosa
        import numpy as np

        results: list[dict[str, Any]] = []
        for track in tracks:
            try:
                file_path = track["file_path"]
                if track.get("synthetic"):
                    y = np.load(file_path)
                    sr: int = track["sample_rate"]
                else:
                    y, sr = librosa.load(file_path, sr=22050, mono=True)

                mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
                centroid = librosa.feature.spectral_centroid(y=y, sr=sr)
                tempo_raw, _ = librosa.beat.beat_track(y=y, sr=sr)

                results.append(
                    {
                        "track_id": track["track_id"],
                        "title": track.get("title", f"track_{track['track_id']}"),
                        "duration_sec": round(len(y) / sr, 3),
                        "sample_rate": sr,
                        "mfcc_mean": mfccs.mean(axis=1).tolist(),
                        "mfcc_std": mfccs.std(axis=1).tolist(),
                        "spectral_centroid_mean": float(centroid.mean()),
                        "spectral_centroid_std": float(centroid.std()),
                        "tempo": float(tempo_raw),
                    }
                )
                log.info("Extracted features for track %s", track["track_id"])
            except Exception as exc:
                log.error("Skipping track %s — %s", track.get("track_id"), exc)

        log.info("Extracted %d / %d tracks", len(results), len(tracks))
        return results

    @task(task_id="load_to_postgres")
    def load_to_postgres(features: list[dict[str, Any]]) -> int:
        """Upsert feature rows into audio_features (idempotent on track_id)."""
        import json

        import psycopg2
        from psycopg2.extras import execute_values

        conn_params = {
            "host": "postgres-warehouse",
            "dbname": os.environ["WAREHOUSE_DB_NAME"],
            "user": os.environ["WAREHOUSE_DB_USER"],
            "password": os.environ["WAREHOUSE_DB_PASSWORD"],
        }

        upsert_sql = """
            INSERT INTO audio_features (
                track_id, title, duration_sec, sample_rate,
                mfcc_mean, mfcc_std,
                spectral_centroid_mean, spectral_centroid_std,
                tempo, processed_at
            ) VALUES %s
            ON CONFLICT (track_id) DO UPDATE SET
                mfcc_mean               = EXCLUDED.mfcc_mean,
                mfcc_std                = EXCLUDED.mfcc_std,
                spectral_centroid_mean  = EXCLUDED.spectral_centroid_mean,
                spectral_centroid_std   = EXCLUDED.spectral_centroid_std,
                tempo                   = EXCLUDED.tempo,
                processed_at            = EXCLUDED.processed_at
        """

        rows = [
            (
                f["track_id"],
                f["title"],
                f["duration_sec"],
                f["sample_rate"],
                json.dumps(f["mfcc_mean"]),
                json.dumps(f["mfcc_std"]),
                f["spectral_centroid_mean"],
                f["spectral_centroid_std"],
                f["tempo"],
                datetime.utcnow(),
            )
            for f in features
        ]

        with psycopg2.connect(**conn_params) as conn, conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows)

        log.info("Upserted %d rows into audio_features", len(rows))
        return len(rows)

    # ── Wire up dependencies ───────────────────────────────────────────────────
    tracks = download_fma_sample()
    features = extract_features(tracks)
    load_to_postgres(features)


# ── Helper functions (run inside task worker processes) ────────────────────────

def _generate_synthetic_tracks(
    sample_size: int, data_dir: Path
) -> list[dict[str, Any]]:
    """Produce synthetic audio signals for dev/CI — no network I/O required."""
    import numpy as np

    tracks: list[dict[str, Any]] = []
    rng = np.random.default_rng(seed=42)

    for i in range(sample_size):
        sr = 22050
        duration = 30
        t = np.linspace(0, duration, sr * duration, endpoint=False)
        freq = rng.uniform(80, 4000)
        y = (
            np.sin(2 * np.pi * freq * t) * 0.5
            + np.sin(2 * np.pi * freq * 2 * t) * 0.25
            + rng.normal(0, 0.05, len(t))
        ).astype(np.float32)

        dest = data_dir / f"synthetic_{i:04d}.npy"
        np.save(dest, y)
        tracks.append(
            {
                "track_id": f"SYN_{i:04d}",
                "title": f"Synthetic Track {i:04d}",
                "file_path": str(dest),
                "sample_rate": sr,
                "synthetic": True,
            }
        )

    log.info("Generated %d synthetic tracks in %s", sample_size, data_dir)
    return tracks


def _stream_download(url: str, dest: Path, chunk_size: int = 8192) -> None:
    import requests

    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                fh.write(chunk)


def _download_fma_tracks(
    sample_size: int, data_dir: Path
) -> list[dict[str, Any]]:
    """Download real FMA small tracks. Needs ~7.2 GB of free disk space.

    FMA metadata zip (~342 MB) is fetched first to get track IDs and titles.
    Individual MP3s are then fetched from the FMA mirror at switch.ch.
    """
    import zipfile

    import pandas as pd

    metadata_dir = data_dir / "metadata"
    metadata_dir.mkdir(exist_ok=True)

    metadata_zip = data_dir / "fma_metadata.zip"
    if not metadata_zip.exists():
        log.info("Downloading FMA metadata (~342 MB) …")
        _stream_download(
            "https://os.unil.cloud.switch.ch/fma/fma_metadata.zip",
            metadata_zip,
        )
        with zipfile.ZipFile(metadata_zip) as zf:
            zf.extractall(metadata_dir)

    tracks_csv = metadata_dir / "fma_metadata" / "tracks.csv"
    df = pd.read_csv(tracks_csv, index_col=0, header=[0, 1])
    sample = df.sample(min(sample_size, len(df)), random_state=42)

    audio_dir = data_dir / "audio"
    audio_dir.mkdir(exist_ok=True)

    tracks: list[dict[str, Any]] = []
    for track_id, row in sample.iterrows():
        tid = str(int(track_id)).zfill(6)
        url = f"https://os.unil.cloud.switch.ch/fma/fma_small/{tid[:3]}/{tid}.mp3"
        dest = audio_dir / f"{tid}.mp3"
        if not dest.exists():
            try:
                _stream_download(url, dest)
            except Exception as exc:
                log.warning("Skipping track %s — %s", tid, exc)
                continue
        tracks.append(
            {
                "track_id": tid,
                "title": str(row.get(("track", "title"), tid)),
                "file_path": str(dest),
                "sample_rate": 22050,
                "synthetic": False,
            }
        )

    return tracks


# Instantiate the DAG
extract_fma_features()
