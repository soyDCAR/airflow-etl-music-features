"""
DAG: extract_fma_features

Pipeline: download → extract → load → dbt (run + test)

Airflow Variables (Admin → Variables):
  FMA_SAMPLE_SIZE     Cantidad de tracks a procesar     (default: 100)
  FMA_DATA_DIR        Path en el contenedor             (default: /tmp/fma)
  FMA_USE_SYNTHETIC   "true" → señales sintéticas       (default: true)
  SLACK_WEBHOOK_URL   Webhook de Slack                  (default: "")
  DBT_PROJECT_DIR     Path al proyecto dbt              (default: /opt/airflow/dbt)

Airflow Connection:
  postgres_warehouse  → creada automáticamente via AIRFLOW_CONN_POSTGRES_WAREHOUSE
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable

from callbacks import notify_failure, notify_sla_miss

log = logging.getLogger(__name__)

_SLA = timedelta(hours=1)

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}

_WAREHOUSE_CONN_ID = "postgres_warehouse"


@dag(
    dag_id="extract_fma_features",
    description="FMA → librosa features → Postgres → dbt",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fma", "audio-features", "etl", "librosa"],
    default_args=_DEFAULT_ARGS,
    sla_miss_callback=notify_sla_miss,
    doc_md=__doc__,
)
def extract_fma_features() -> None:

    @task(task_id="download_fma_sample", sla=_SLA)
    def download_fma_sample() -> list[dict[str, Any]]:
        """Genera tracks sintéticos o descarga MP3s reales según FMA_USE_SYNTHETIC."""
        use_synthetic = Variable.get("FMA_USE_SYNTHETIC", default_var="true").lower() == "true"
        sample_size = int(Variable.get("FMA_SAMPLE_SIZE", default_var="100"))
        data_dir = Path(Variable.get("FMA_DATA_DIR", default_var="/tmp/fma"))
        data_dir.mkdir(parents=True, exist_ok=True)
        if use_synthetic:
            return _generate_synthetic_tracks(sample_size, data_dir)
        return _download_fma_tracks(sample_size, data_dir)

    @task(task_id="extract_features", sla=_SLA)
    def extract_features(tracks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """Extrae MFCC (13 coef), centroid espectral y tempo por track.
        Retorna {"features": [...], "errors": [...]} para persistir ambos."""
        import librosa
        import numpy as np

        features: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

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

                features.append({
                    "track_id": track["track_id"],
                    "title": track.get("title", f"track_{track['track_id']}"),
                    "duration_sec": round(len(y) / sr, 3),
                    "sample_rate": sr,
                    "mfcc_mean": mfccs.mean(axis=1).tolist(),
                    "mfcc_std": mfccs.std(axis=1).tolist(),
                    "spectral_centroid_mean": float(centroid.mean()),
                    "spectral_centroid_std": float(centroid.std()),
                    "tempo": float(tempo_raw),
                })
            except Exception as exc:  # noqa: BLE001
                log.error("Fallo extracción track %s: %s", track.get("track_id"), exc)
                errors.append({"track_id": track.get("track_id", "unknown"), "error": str(exc)})

        log.info("extract_features: OK=%d  FAIL=%d  TOTAL=%d", len(features), len(errors), len(tracks))
        return {"features": features, "errors": errors}

    @task(task_id="load_to_postgres", sla=_SLA)
    def load_to_postgres(payload: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        """Upsert de features + insert de errores. Usa PostgresHook (sin credenciales en código)."""
        import json

        from airflow.operators.python import get_current_context
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values

        ctx = get_current_context()
        dag_id = ctx["dag"].dag_id
        run_id = ctx["run_id"]
        task_id = ctx["task_instance"].task_id

        features = payload["features"]
        errors = payload["errors"]
        hook = PostgresHook(postgres_conn_id=_WAREHOUSE_CONN_ID)

        upsert_sql = """
            INSERT INTO audio_features (
                track_id, title, duration_sec, sample_rate,
                mfcc_mean, mfcc_std, spectral_centroid_mean, spectral_centroid_std,
                tempo, processed_at
            ) VALUES %s
            ON CONFLICT (track_id) DO UPDATE SET
                mfcc_mean=EXCLUDED.mfcc_mean, mfcc_std=EXCLUDED.mfcc_std,
                spectral_centroid_mean=EXCLUDED.spectral_centroid_mean,
                spectral_centroid_std=EXCLUDED.spectral_centroid_std,
                tempo=EXCLUDED.tempo, processed_at=EXCLUDED.processed_at
        """
        error_sql = """
            INSERT INTO pipeline_errors (dag_id, run_id, task_id, track_id, error_message)
            VALUES %s
        """
        feature_rows = [(f["track_id"], f["title"], f["duration_sec"], f["sample_rate"],
                         json.dumps(f["mfcc_mean"]), json.dumps(f["mfcc_std"]),
                         f["spectral_centroid_mean"], f["spectral_centroid_std"],
                         f["tempo"], datetime.utcnow()) for f in features]
        error_rows = [(dag_id, run_id, task_id, e["track_id"], e["error"]) for e in errors]

        with hook.get_conn() as conn, conn.cursor() as cur:
            if feature_rows:
                execute_values(cur, upsert_sql, feature_rows)
            if error_rows:
                execute_values(cur, error_sql, error_rows)

        log.info("load_to_postgres: upserted=%d  errors_logged=%d", len(feature_rows), len(error_rows))
        return {"loaded": len(feature_rows), "errors_logged": len(error_rows)}

    @task(task_id="run_dbt_transforms", sla=timedelta(hours=2))
    def run_dbt_transforms(load_result: dict[str, int]) -> dict[str, Any]:
        """Corre dbt run + dbt test. Falla si cualquiera retorna código != 0."""
        import subprocess

        dbt_dir = Path(Variable.get("DBT_PROJECT_DIR", default_var="/opt/airflow/dbt"))

        def _dbt(subcmd: str) -> str:
            cmd = ["dbt", subcmd, "--profiles-dir", str(dbt_dir),
                   "--project-dir", str(dbt_dir), "--no-use-colors"]
            log.info("Corriendo: %s", " ".join(cmd))
            result = subprocess.run(cmd, capture_output=True, text=True)
            log.info("stdout:\n%s", result.stdout)
            if result.returncode != 0:
                raise RuntimeError(f"`dbt {subcmd}` falló (exit {result.returncode}):\n{result.stderr}")
            return result.stdout

        _dbt("run")
        _dbt("test")
        return {"dbt_run": "ok", "dbt_test": "ok", "upstream": load_result}

    # ── Wiring ────────────────────────────────────────────────────────────────
    tracks = download_fma_sample()
    payload = extract_features(tracks)
    load_result = load_to_postgres(payload)
    run_dbt_transforms(load_result)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _generate_synthetic_tracks(sample_size: int, data_dir: Path) -> list[dict[str, Any]]:
    import numpy as np
    rng = np.random.default_rng(seed=42)
    tracks: list[dict[str, Any]] = []
    for i in range(sample_size):
        sr, duration = 22050, 30
        t = np.linspace(0, duration, sr * duration, endpoint=False)
        freq = rng.uniform(80, 4000)
        y = (np.sin(2 * np.pi * freq * t) * 0.5
             + np.sin(2 * np.pi * freq * 2 * t) * 0.25
             + rng.normal(0, 0.05, len(t))).astype(np.float32)
        dest = data_dir / f"synthetic_{i:04d}.npy"
        np.save(dest, y)
        tracks.append({"track_id": f"SYN_{i:04d}", "title": f"Synthetic Track {i:04d}",
                        "file_path": str(dest), "sample_rate": sr, "synthetic": True})
    log.info("Generados %d tracks sintéticos en %s", sample_size, data_dir)
    return tracks


def _stream_download(url: str, dest: Path, chunk_size: int = 8192) -> None:
    import requests
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                fh.write(chunk)


def _download_fma_tracks(sample_size: int, data_dir: Path) -> list[dict[str, Any]]:
    import zipfile

    import pandas as pd
    metadata_dir = data_dir / "metadata"
    metadata_dir.mkdir(exist_ok=True)
    metadata_zip = data_dir / "fma_metadata.zip"
    if not metadata_zip.exists():
        log.info("Descargando FMA metadata (~342 MB)…")
        _stream_download("https://os.unil.cloud.switch.ch/fma/fma_metadata.zip", metadata_zip)
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
                log.warning("Skip track %s: %s", tid, exc)
                continue
        tracks.append({"track_id": tid, "title": str(row.get(("track", "title"), tid)),
                        "file_path": str(dest), "sample_rate": 22050, "synthetic": False})
    return tracks


extract_fma_features()
