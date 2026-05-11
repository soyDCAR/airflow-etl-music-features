-- ─────────────────────────────────────────────────────────────────────────────
-- Warehouse schema: music audio features
-- Runs automatically when postgres-warehouse first starts (initdb.d hook).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS audio_features (
    id                      BIGSERIAL       PRIMARY KEY,
    track_id                VARCHAR(20)     NOT NULL UNIQUE,
    title                   TEXT,
    duration_sec            FLOAT,
    sample_rate             INTEGER,

    -- MFCC vectors: 13 coefficients stored as JSON arrays
    mfcc_mean               JSONB           NOT NULL,
    mfcc_std                JSONB           NOT NULL,

    -- Spectral features
    spectral_centroid_mean  FLOAT,
    spectral_centroid_std   FLOAT,

    -- Rhythm
    tempo                   FLOAT,

    -- Housekeeping
    processed_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_af_track_id      ON audio_features (track_id);
CREATE INDEX IF NOT EXISTS idx_af_tempo         ON audio_features (tempo);
CREATE INDEX IF NOT EXISTS idx_af_processed_at  ON audio_features (processed_at DESC);

COMMENT ON TABLE  audio_features                       IS 'Acoustic feature vectors extracted from FMA audio tracks';
COMMENT ON COLUMN audio_features.track_id              IS 'FMA track ID or synthetic ID (SYN_XXXX)';
COMMENT ON COLUMN audio_features.mfcc_mean             IS 'Mean of 13 MFCC coefficients across time frames';
COMMENT ON COLUMN audio_features.mfcc_std              IS 'Std dev of 13 MFCC coefficients across time frames';
COMMENT ON COLUMN audio_features.spectral_centroid_mean IS 'Mean spectral centroid in Hz';
COMMENT ON COLUMN audio_features.tempo                 IS 'Estimated BPM via librosa beat tracker';
