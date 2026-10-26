CREATE TABLE IF NOT EXISTS marts.earnings_events (
    ticker          TEXT             NOT NULL,
    announced_at    TIMESTAMPTZ      NOT NULL,
    timing          TEXT             NOT NULL,
    reaction_date   DATE             NOT NULL,
    eps_estimate    DOUBLE PRECISION,
    eps_reported    DOUBLE PRECISION,
    eps_surprise    DOUBLE PRECISION,
    surprise_pct    DOUBLE PRECISION,
    surprise_rank   DOUBLE PRECISION,
    car_pre         DOUBLE PRECISION,
    ret_day0        DOUBLE PRECISION,
    car_0_1         DOUBLE PRECISION,
    car_drift       DOUBLE PRECISION,
    built_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, announced_at)
);