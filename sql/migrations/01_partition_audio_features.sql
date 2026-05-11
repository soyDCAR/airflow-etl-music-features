-- Migration 01: Convert audio_features to a range-partitioned table
-- Partition by processed_at (monthly buckets) to keep query latency flat
-- as the dataset grows.
--
-- Run ONCE manually after initial data load:
--   psql -h localhost -p 5433 -U warehouse -d music_features -f 01_partition_audio_features.sql
--
-- Safety: wraps everything in a transaction — rolls back on any error.

BEGIN;

-- 1. Rename existing table so we keep the data
ALTER TABLE IF EXISTS public.audio_features
    RENAME TO audio_features_old;

-- 2. Create the partitioned parent (same columns)
CREATE TABLE public.audio_features (
    track_id              TEXT        NOT NULL,
    title                 TEXT,
    duration_sec          DOUBLE PRECISION,
    sample_rate           INTEGER,
    mfcc_mean             DOUBLE PRECISION,
    mfcc_std              DOUBLE PRECISION,
    spectral_centroid_mean DOUBLE PRECISION,
    spectral_centroid_std  DOUBLE PRECISION,
    tempo                 DOUBLE PRECISION,
    tempo_bucket          TEXT,
    duration_bucket       TEXT,
    processed_month       TEXT,
    processed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (processed_at);

-- 3. Create partitions: one per month from 2024-01 onwards
--    Add more as needed; Postgres will raise an error for out-of-range rows.
CREATE TABLE public.audio_features_y2024m01
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE public.audio_features_y2024m02
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE public.audio_features_y2024m03
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

CREATE TABLE public.audio_features_y2025m01
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE public.audio_features_y2025m02
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE public.audio_features_y2025m03
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE public.audio_features_y2025m04
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE public.audio_features_y2025m05
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE public.audio_features_y2025m06
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2025-06-01') TO ('2026-01-01');

CREATE TABLE public.audio_features_y2026m01
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE public.audio_features_y2026m02
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE public.audio_features_y2026m03
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE public.audio_features_y2026m04
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE public.audio_features_y2026m05
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE public.audio_features_y2026m06
    PARTITION OF public.audio_features
    FOR VALUES FROM ('2026-06-01') TO ('2027-01-01');

-- 4. Re-create unique index on the partitioned table
CREATE UNIQUE INDEX audio_features_track_id_processed_at_idx
    ON public.audio_features (track_id, processed_at);

-- 5. Copy data from old table
INSERT INTO public.audio_features
SELECT * FROM public.audio_features_old;

-- 6. Drop backup (comment out if you prefer to keep it temporarily)
-- DROP TABLE public.audio_features_old;

COMMIT;
