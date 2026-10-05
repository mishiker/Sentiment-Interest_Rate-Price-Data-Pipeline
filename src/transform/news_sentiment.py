import logging
from datetime import datetime, timezone
from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.dialects.postgresql import insert
from utils.db import get_engine

log = logging.getLogger(__name__)

MODEL = "ProsusAI/finbert"

BATCH_SIZE = 32

WRITE_EVERY = 1000


def get_tables(engine) -> tuple[Table, Table]:
    finnhub_news = Table("finnhub_news", MetaData(schema="raw"), autoload_with=engine)
    news_sentiment = Table("news_sentiment", MetaData(schema="features"), autoload_with=engine)

    return finnhub_news, news_sentiment


def get_unscored(conn, finnhub_news: Table, news_sentiment: Table) -> list[tuple[int, str]]:
    already_scored = (
        select(news_sentiment.c.news_id)
        .where(
            news_sentiment.c.news_id == finnhub_news.c.news_id,
            news_sentiment.c.model == MODEL,
        )
        .exists()
    )
    query = (
        select(finnhub_news.c.news_id, func.min(finnhub_news.c.headline))
        .where(~already_scored)
        .group_by(finnhub_news.c.news_id)
        .order_by(finnhub_news.c.news_id)
    )

    return conn.execute(query).all()


def score(classifier, headlines: list[str]) -> list[dict[str, float]]:
    results = classifier(headlines, batch_size=BATCH_SIZE, truncation=True)

    return [{s["label"]: s["score"] for s in scores} for scores in results]


def to_rows(articles: list[tuple[int, str]], scores: list[dict[str, float]],
            scored_at: datetime) -> list[dict]:
    return [
        {
            "news_id": news_id,
            "model": MODEL,
            "positive": probs["positive"],
            "negative": probs["negative"],
            "neutral": probs["neutral"],
            "label": max(probs, key=probs.get),
            "score": probs["positive"] - probs["negative"],
            "scored_at": scored_at,
        }
        for (news_id, _), probs in zip(articles, scores)
    ]


def build_insert(news_sentiment: Table):
    return (
        insert(news_sentiment)
        .on_conflict_do_nothing(index_elements=["news_id", "model"])
        .returning(news_sentiment.c.news_id)
    )


def run() -> None:
    scored_at = datetime.now(timezone.utc)
    engine = get_engine()
    finnhub_news, news_sentiment = get_tables(engine)

    with engine.connect() as conn:
        articles = get_unscored(conn, finnhub_news, news_sentiment)

    if not articles:
        log.info("Brak nowych artykułów do oceny")
        return

    log.info("Do oceny: %d artykułów, model %s", len(articles), MODEL)
    from transformers import pipeline  # ładuje torch, co trwa kilka sekund

    classifier = pipeline("text-classification", model=MODEL, top_k=None)
    stmt = build_insert(news_sentiment)
    total = 0

    for start in range(0, len(articles), WRITE_EVERY):
        chunk = articles[start:start + WRITE_EVERY]
        scores = score(classifier, [headline for _, headline in chunk])
        rows = to_rows(chunk, scores, scored_at)

        with engine.begin() as conn:
            total += len(conn.execute(stmt, rows).all())

        log.info("Ocenione %d / %d", start + len(chunk), len(articles))

    log.info("Zapisano %d ocen", total)
