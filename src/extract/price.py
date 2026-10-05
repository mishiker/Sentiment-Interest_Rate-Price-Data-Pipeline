import json
import logging
import pandas as pd
import yfinance as yf
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import MetaData, Table, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from config import BENCHMARK, TICKERS
from utils.db import get_engine

log = logging.getLogger(__name__)

PRICE_TICKERS = [BENCHMARK, *TICKERS]

BACKFILL_START = date(2020, 1, 1)

LOOKBACK_DAYS = 7

PRICE_COLUMNS = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}

PRICE_FIELDS = ["open", "high", "low", "close", "adj_close", "volume"]


def extract(tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    # yfinance traktuje `end` jako wyłączny, stąd +1 dzień
    df = yf.download(
        tickers,
        start=start,
        end=end + timedelta(days=1),
        auto_adjust=False,
        actions=False,
        group_by="ticker",
        progress=False,
        threads=False,
    )
    if df.empty:
        log.warning("yfinance zwrócił pustą odpowiedź")
        return {}

    available = set(df.columns.get_level_values(0))
    frames = {}

    for ticker in tickers:
        if ticker not in available:
            log.warning("Brak danych dla %s", ticker)
            continue

        frame = df[ticker].dropna(subset=["Close"])

        if frame.empty:
            log.warning("Same puste wiersze dla %s", ticker)
            continue

        frames[ticker] = frame

    return frames


def _num(value):
    return None if pd.isna(value) else float(value)


def _int(value):
    return None if pd.isna(value) else int(value)


def to_payload(frame: pd.DataFrame) -> list[dict]:
    return json.loads(frame.reset_index().to_json(orient="records", date_format="iso"))


def get_tables(engine) -> tuple[Table, Table]:
    metadata = MetaData(schema="raw")
    stock_prices = Table("stock_prices", metadata, autoload_with=engine)
    api_responses = Table("api_responses", metadata, autoload_with=engine)
    
    return stock_prices, api_responses


def get_start_date(conn, stock_prices: Table, tickers: list[str]) -> date:
    query = (
        select(stock_prices.c.ticker, func.max(stock_prices.c.price_date))
        .where(stock_prices.c.ticker.in_(tickers))
        .group_by(stock_prices.c.ticker)
    )
    last_dates = dict(conn.execute(query).all())
    missing = set(tickers) - set(last_dates)

    if missing:
        log.info("Brak historii dla %s, pełny backfill od %s", sorted(missing), BACKFILL_START)
        return BACKFILL_START
    
    return min(last_dates.values()) - timedelta(days=LOOKBACK_DAYS)


def to_rows(ticker: str, frame: pd.DataFrame, ingested_at: datetime) -> list[dict]:
    frame = frame.rename(columns=PRICE_COLUMNS)

    return [
        {
            "ticker": ticker,
            "price_date": price_date.date(),
            "open": _num(row["open"]),
            "high": _num(row["high"]),
            "low": _num(row["low"]),
            "close": _num(row["close"]),
            "adj_close": _num(row["adj_close"]),
            "volume": _int(row["volume"]),
            "ingested_at": ingested_at,
        }
        for price_date, row in frame.iterrows()
    ]


def build_upsert(stock_prices: Table):
    stmt = insert(stock_prices)
    changed = or_(*(stock_prices.c[f].is_distinct_from(stmt.excluded[f]) for f in PRICE_FIELDS))

    return stmt.on_conflict_do_update(
        index_elements=["ticker", "price_date"],
        set_={f: stmt.excluded[f] for f in [*PRICE_FIELDS, "ingested_at"]},
        where=changed,
    ).returning(stock_prices.c.price_date)


def load(conn, stock_prices: Table, api_responses: Table,
         frames: dict[str, pd.DataFrame], ingested_at: datetime) -> int:
    upsert = build_upsert(stock_prices)
    total = 0

    for ticker, frame in frames.items():
        conn.execute(
            api_responses.insert(),
            {
                "source": "yfinance",
                "endpoint": f"download/{ticker}",
                "payload": to_payload(frame),
                "ingested_at": ingested_at,
            },
        )
        rows = to_rows(ticker, frame, ingested_at)
        conn.execute(upsert, rows)
        total += len(rows)
        log.info("%s: %d wierszy", ticker, len(rows))
    
    return total


def run(tickers: list[str] = PRICE_TICKERS) -> None:
    ingested_at = datetime.now(timezone.utc)
    engine = get_engine()
    stock_prices, api_responses = get_tables(engine)

    with engine.connect() as conn:
        start = get_start_date(conn, stock_prices, tickers)
    
    end = date.today()
    log.info("Pobieranie %d spółek od %s do %s", len(tickers), start, end)

    frames = extract(tickers, start, end)

    if not frames:
        return

    with engine.begin() as conn:  # jedna transakcja: commit przy sukcesie, rollback przy błędzie
        total = load(conn, stock_prices, api_responses, frames, ingested_at)
    
    log.info("Zapisano %d wierszy dla %d spółek", total, len(frames))


if __name__ == "__main__":
    run()
