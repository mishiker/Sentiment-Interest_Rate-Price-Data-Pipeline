import json
import logging
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from sqlalchemy import MetaData, Table, or_, select
from sqlalchemy.dialects.postgresql import insert
from config import TICKERS
from utils.db import get_engine

log = logging.getLogger(__name__)

# limit obejmuje też około 4 przyszłych terminów bez wyniku
BACKFILL_QUARTERS = 32

UPDATE_QUARTERS = 8

REQUEST_INTERVAL = 1.0

EARNINGS_COLUMNS = {
    "EPS Estimate": "eps_estimate",
    "Reported EPS": "eps_reported",
    "Surprise(%)": "surprise_pct",
}

EARNINGS_FIELDS = list(EARNINGS_COLUMNS.values())


def extract(ticker: str, limit: int) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).get_earnings_dates(limit=limit)
    except Exception as exc:  # yfinance nie ma wspólnej klasy błędów, a Yahoo często zmienia odpowiedzi
        raise RuntimeError(f"{ticker}: błąd yfinance ({type(exc).__name__}: {exc})") from exc

    if df is None or df.empty:
        return pd.DataFrame()

    missing = set(EARNINGS_COLUMNS) - set(df.columns)
    if missing:
        raise RuntimeError(f"{ticker}: brak kolumn {sorted(missing)}, Yahoo mogło zmienić format")

    # przyszłe terminy nie mają jeszcze wyniku, a ich godzina potrafi się zmieniać
    return df.dropna(subset=["Reported EPS"])


def _num(value):
    return None if pd.isna(value) else float(value)


def to_payload(frame: pd.DataFrame) -> list[dict]:
    return json.loads(frame.reset_index().to_json(orient="records", date_format="iso"))


def get_tables(engine) -> tuple[Table, Table]:
    metadata = MetaData(schema="raw")
    earnings = Table("earnings", metadata, autoload_with=engine)
    api_responses = Table("api_responses", metadata, autoload_with=engine)

    return earnings, api_responses


def get_known_tickers(conn, earnings: Table, tickers: list[str]) -> set[str]:
    query = select(earnings.c.ticker).where(earnings.c.ticker.in_(tickers)).distinct()

    return set(conn.execute(query).scalars())


def to_rows(ticker: str, frame: pd.DataFrame, ingested_at: datetime) -> list[dict]:
    return [
        {
            "ticker": ticker,
            "announced_at": announced_at.to_pydatetime(),
            **{column: _num(row[source]) for source, column in EARNINGS_COLUMNS.items()},
            "ingested_at": ingested_at,
        }
        for announced_at, row in frame.iterrows()
    ]


def build_upsert(earnings: Table):
    stmt = insert(earnings)
    changed = or_(*(earnings.c[f].is_distinct_from(stmt.excluded[f]) for f in EARNINGS_FIELDS))

    return stmt.on_conflict_do_update(
        index_elements=["ticker", "announced_at"],
        set_={f: stmt.excluded[f] for f in [*EARNINGS_FIELDS, "ingested_at"]},
        where=changed,
    ).returning(earnings.c.announced_at)


def load(conn, earnings: Table, api_responses: Table,
         frames: dict[str, pd.DataFrame], ingested_at: datetime) -> int:
    upsert = build_upsert(earnings)
    total = 0

    for ticker, frame in frames.items():
        conn.execute(
            api_responses.insert(),
            {
                "source": "yfinance",
                "endpoint": f"earnings_dates/{ticker}",
                "payload": to_payload(frame),
                "ingested_at": ingested_at,
            },
        )
        rows = to_rows(ticker, frame, ingested_at)
        changed = len(conn.execute(upsert, rows).all())
        total += changed
        log.info("%s: %d kwartałów, %d nowych lub zmienionych", ticker, len(rows), changed)

    return total


def run(tickers: list[str] = TICKERS) -> None:
    ingested_at = datetime.now(timezone.utc)
    engine = get_engine()
    earnings, api_responses = get_tables(engine)

    with engine.connect() as conn:
        known = get_known_tickers(conn, earnings, tickers)

    frames = {}
    failed = []

    for ticker in tickers:
        limit = UPDATE_QUARTERS if ticker in known else BACKFILL_QUARTERS
        try:
            frame = extract(ticker, limit)
        except RuntimeError as exc:
            log.error("%s", exc)
            failed.append(ticker)
            continue
        finally:
            time.sleep(REQUEST_INTERVAL)

        if frame.empty:
            log.warning("%s: brak opublikowanych wyników", ticker)
            continue

        frames[ticker] = frame

    if frames:
        with engine.begin() as conn:  # jedna transakcja: commit przy sukcesie, rollback przy błędzie
            total = load(conn, earnings, api_responses, frames, ingested_at)

        log.info("Zapisano %d wierszy dla %d spółek", total, len(frames))

    if failed:
        raise RuntimeError(f"Nie udało się pobrać wyników dla: {failed}")