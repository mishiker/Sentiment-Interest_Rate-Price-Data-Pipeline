CREATE SCHEMA IF NOT EXISTS features;

CREATE TABLE IF NOT EXISTS features.news_sentiment (
    news_id    BIGINT      NOT NULL,
    model      TEXT        NOT NULL,
    positive   REAL        NOT NULL,
    negative   REAL        NOT NULL,
    neutral    REAL        NOT NULL,
    label      TEXT        NOT NULL,
    score      REAL        NOT NULL,
    scored_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (news_id, model)
);