CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.fred_observations (
    series_id    TEXT        NOT NULL,
    obs_date     DATE        NOT NULL,
    value        NUMERIC,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, obs_date)
);

CREATE TABLE IF NOT EXISTS raw.stock_prices (
    ticker       TEXT        NOT NULL,
    price_date   DATE        NOT NULL,
    open         NUMERIC,
    high         NUMERIC,
    low          NUMERIC,
    close        NUMERIC,
    adj_close    NUMERIC,
    volume       BIGINT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, price_date)
);

CREATE TABLE IF NOT EXISTS raw.api_responses (
    id           BIGSERIAL   PRIMARY KEY,
    source       TEXT        NOT NULL,
    endpoint     TEXT,
    payload      JSONB       NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);