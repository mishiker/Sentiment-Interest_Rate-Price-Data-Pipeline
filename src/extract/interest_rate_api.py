import logging
import os
import pandas as pd
import requests
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.dialects.postgresql import insert
from utils.db import get_engine

log = logging.getLogger(__name__)

SERIES = [
    "DGS2",          # rentowność 2Y
    "DGS10",         # rentowność 10Y
    "T10Y2Y",        # spread 10Y - 2Y
    "DFII10",        # realna rentowność 10Y (TIPS)
    "DFF",           # efektywna stopa Fed Funds (dzienna)
    "VIXCLS",        # VIX
    "BAMLH0A0HYM2",  # spread obligacji high yield
]

BACKFILL_START = date(2020, 1, 1)

LOOKBACK_DAYS = 7

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

REQUEST_TIMEOUT = 30


def extract(series_id: str, start: date, end: date, api_key: str) -> dict:
    try:
        response = requests.get(
            FRED_URL,
            params={
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "observation_start": start.isoformat(),
                "observation_end": end.isoformat(),
            },
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        # komunikaty wyjątków requests zawierają pełny URL razem z api_key
        raise RuntimeError(f"{series_id}: błąd połączenia ({type(exc).__name__})") from None

    if not response.ok:
        raise RuntimeError(f"{series_id}: HTTP {response.status_code} {response.text[:200]}")
    
    return response.json()


def get_tables(engine) -> tuple[Table, Table]:
    metadata = MetaData(schema="raw")
    fred_observations = Table("fred_observations", metadata, autoload_with=engine)
    api_responses = Table("api_responses", metadata, autoload_with=engine)

    return fred_observations, api_responses


def get_start_dates(conn, fred_observations: Table, series: list[str]) -> dict[str, date]:
    query = (
        select(fred_observations.c.series_id, func.max(fred_observations.c.obs_date))
        .where(fred_observations.c.series_id.in_(series))
        .group_by(fred_observations.c.series_id)
    )
    last_dates = dict(conn.execute(query).all())
    starts = {}

    for series_id in series:
        if series_id in last_dates:
            starts[series_id] = last_dates[series_id] - timedelta(days=LOOKBACK_DAYS)
        else:
            log.info("Brak historii dla %s, pełny backfill od %s", series_id, BACKFILL_START)
            starts[series_id] = BACKFILL_START

    return starts


def to_rows(series_id: str, payload: dict, ingested_at: datetime) -> list[dict]:
    observations = pd.DataFrame(payload.get("observations", []), columns=["date", "value"])
    # FRED oznacza dni bez notowań wartością "."
    observations["value"] = pd.to_numeric(observations["value"], errors="coerce")
    observations = observations.dropna(subset=["value"])

    return [
        {
            "series_id": series_id,
            "obs_date": date.fromisoformat(obs.date),
            "value": float(obs.value),
            "ingested_at": ingested_at,
        }

        for obs in observations.itertuples(index=False)
    ]


def build_upsert(fred_observations: Table):
    stmt = insert(fred_observations)

    return stmt.on_conflict_do_update(
        index_elements=["series_id", "obs_date"],
        set_={"value": stmt.excluded.value, "ingested_at": stmt.excluded.ingested_at},
        where=fred_observations.c.value.is_distinct_from(stmt.excluded.value),
    ).returning(fred_observations.c.obs_date)


def load(conn, fred_observations: Table, api_responses: Table,
         payloads: dict[str, dict], ingested_at: datetime) -> int:
    upsert = build_upsert(fred_observations)
    total = 0

    for series_id, payload in payloads.items():
        conn.execute(
            api_responses.insert(),
            {
                "source": "fred",
                "endpoint": f"series/observations/{series_id}",
                "payload": payload,
                "ingested_at": ingested_at,
            },
        )
        rows = to_rows(series_id, payload, ingested_at)
        changed = len(conn.execute(upsert, rows).all()) if rows else 0

        total += changed
        log.info("%s: %d pobranych, %d nowych lub zmienionych", series_id, len(rows), changed)

    return total


def run(series: list[str] = SERIES) -> None:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY is not set in .env")

    ingested_at = datetime.now(timezone.utc)
    engine = get_engine()
    fred_observations, api_responses = get_tables(engine)

    with engine.connect() as conn:
        starts = get_start_dates(conn, fred_observations, series)

    end = date.today()
    payloads = {}
    failed = []

    for series_id in series:
        log.info("%s: pobieranie od %s do %s", series_id, starts[series_id], end)
        try:
            payloads[series_id] = extract(series_id, starts[series_id], end, api_key)
        except RuntimeError as exc:
            log.error("%s", exc)
            failed.append(series_id)

    if payloads:
        with engine.begin() as conn:  # jedna transakcja: commit przy sukcesie, rollback przy błędzie
            total = load(conn, fred_observations, api_responses, payloads, ingested_at)

        log.info("Zapisano %d wierszy dla %d serii", total, len(payloads))

    if failed:
        raise RuntimeError(f"Nie udało się pobrać serii: {failed}")
