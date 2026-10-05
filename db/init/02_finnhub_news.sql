CREATE TABLE IF NOT EXISTS raw.finnhub_news (
    ticker        TEXT        NOT NULL,
    news_id       BIGINT      NOT NULL,
    published_at  TIMESTAMPTZ NOT NULL,
    headline      TEXT        NOT NULL,
    summary       TEXT,
    source        TEXT,
    url           TEXT,
    category      TEXT,
    related       TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, news_id)
);

CREATE INDEX IF NOT EXISTS finnhub_news_ticker_published_idx
    ON raw.finnhub_news (ticker, published_at);