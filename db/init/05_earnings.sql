CREATE TABLE IF NOT EXISTS raw.earnings (
    ticker        TEXT        NOT NULL,
    announced_at  TIMESTAMPTZ NOT NULL,
    eps_estimate  NUMERIC,
    eps_reported  NUMERIC,
    surprise_pct  NUMERIC,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, announced_at)
);