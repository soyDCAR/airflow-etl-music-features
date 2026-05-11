-- Tabla de errores a nivel de track individual por run.
CREATE TABLE IF NOT EXISTS pipeline_errors (
    id              BIGSERIAL       PRIMARY KEY,
    dag_id          TEXT            NOT NULL,
    run_id          TEXT            NOT NULL,
    task_id         TEXT            NOT NULL,
    track_id        TEXT,
    error_message   TEXT,
    occurred_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pe_dag_run ON pipeline_errors (dag_id, run_id);
CREATE INDEX IF NOT EXISTS idx_pe_occurred_at ON pipeline_errors (occurred_at DESC);
COMMENT ON TABLE pipeline_errors IS 'Errores por track capturados en extract_features';
