import logging
import os
import time
import requests
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.dialects.postgresql import insert
from config import TICKERS
from utils.db import get_engine

log = logging.getLogger(__name__)

BACKFILL_DAYS = 365  # darmowy plan Finnhub udostępnia około roku historii

LOOKBACK_DAYS = 2

CHUNK_DAYS = 7

FINNHUB_URL = "https://finnhub.io/api/v1/company-news"

REQUEST_TIMEOUT = 30

REQUEST_INTERVAL = 1.1  # darmowy plan: 60 zapytań na minutę

RATE_LIMIT_WAIT = 60

MAX_RETRIES = 3


def date_chunks(start: date, end: date):
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def extract(ticker: str, start: date, end: date, api_key: str) -> list[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                FINNHUB_URL,
                params={"symbol": ticker, "from": start.isoformat(), "to": end.isoformat()},
                headers={"X-Finnhub-Token": api_key},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"{ticker}: błąd połączenia ({type(exc).__name__})") from exc

        if response.status_code == 429 and attempt < MAX_RETRIES:
            log.warning("%s: przekroczony limit zapytań, ponowienie za %d s", ticker, RATE_LIMIT_WAIT)
            time.sleep(RATE_LIMIT_WAIT)
            continue

        if not response.ok:
            raise RuntimeError(f"{ticker}: HTTP {response.status_code} {response.text[:200]}")

        return response.json()


def get_tables(engine) -> tuple[Table, Table]:
    metadata = MetaData(schema="raw")
    finnhub_news = Table("finnhub_news", metadata, autoload_with=engine)
    api_responses = Table("api_responses", metadata, autoload_with=engine)

    return finnhub_news, api_responses


def get_start_dates(conn, finnhub_news: Table, tickers: list[str]) -> dict[str, date]:
    query = (
        select(finnhub_news.c.ticker, func.max(finnhub_news.c.published_at))
        .where(finnhub_news.c.ticker.in_(tickers))
        .group_by(finnhub_news.c.ticker)
    )
    last_published = dict(conn.execute(query).all())
    backfill_start = date.today() - timedelta(days=BACKFILL_DAYS)
    starts = {}

    for ticker in tickers:
        if ticker in last_published:
            starts[ticker] = last_published[ticker].date() - timedelta(days=LOOKBACK_DAYS)
        else:
            log.info("Brak historii dla %s, pełny backfill od %s", ticker, backfill_start)
            starts[ticker] = backfill_start

    return starts


def to_rows(ticker: str, articles: list[dict], ingested_at: datetime) -> list[dict]:
    return [
        {
            "ticker": ticker,
            "news_id": article["id"],
            "published_at": datetime.fromtimestamp(article["datetime"], tz=timezone.utc),
            "headline": article["headline"],
            "summary": article.get("summary") or None,
            "source": article.get("source") or None,
            "url": article.get("url") or None,
            "category": article.get("category") or None,
            "related": article.get("related") or None,
            "ingested_at": ingested_at,
        }
        for article in articles
        if article.get("id") and article.get("headline") and article.get("datetime")
    ]


def build_insert(finnhub_news: Table):
    return (
        insert(finnhub_news)
        .on_conflict_do_nothing(index_elements=["ticker", "news_id"])
        .returning(finnhub_news.c.news_id)
    )


def load(conn, finnhub_news: Table, api_responses: Table,
         responses: dict[str, list[tuple[date, date, list[dict]]]], ingested_at: datetime) -> int:
    stmt = build_insert(finnhub_news)
    total = 0

    for ticker, chunks in responses.items():
        fetched = 0
        new = 0

        for chunk_start, chunk_end, articles in chunks:
            conn.execute(
                api_responses.insert(),
                {
                    "source": "finnhub",
                    "endpoint": f"company-news/{ticker}?from={chunk_start}&to={chunk_end}",
                    "payload": articles,
                    "ingested_at": ingested_at,
                },
            )
            rows = to_rows(ticker, articles, ingested_at)
            fetched += len(rows)
            new += len(conn.execute(stmt, rows).all()) if rows else 0

        total += new
        log.info("%s: %d pobranych, %d nowych", ticker, fetched, new)

    return total


def run(tickers: list[str] = TICKERS) -> None:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY is not set in .env")

    ingested_at = datetime.now(timezone.utc)
    engine = get_engine()
    finnhub_news, api_responses = get_tables(engine)

    with engine.connect() as conn:
        starts = get_start_dates(conn, finnhub_news, tickers)

    end = date.today()
    responses = {}
    failed = []

    for ticker in tickers:
        log.info("%s: pobieranie od %s do %s", ticker, starts[ticker], end)
        try:
            chunks = []
            for chunk_start, chunk_end in date_chunks(starts[ticker], end):
                chunks.append((chunk_start, chunk_end, extract(ticker, chunk_start, chunk_end, api_key)))
                time.sleep(REQUEST_INTERVAL)
            responses[ticker] = chunks
        except RuntimeError as exc:
            log.error("%s", exc)
            failed.append(ticker)

    if responses:
        with engine.begin() as conn:  # jedna transakcja: commit przy sukcesie, rollback przy błędzie
            total = load(conn, finnhub_news, api_responses, responses, ingested_at)

        log.info("Zapisano %d nowych artykułów dla %d spółek", total, len(responses))

    if failed:
        raise RuntimeError(f"Nie udało się pobrać newsów dla: {failed}")
