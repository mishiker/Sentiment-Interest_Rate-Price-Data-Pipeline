import logging
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import MetaData, Table, and_, delete, select
from config import BENCHMARK, TICKERS
from transform.news_sentiment import MODEL
from utils.db import get_engine

log = logging.getLogger(__name__)

MARKET_TZ = "America/New_York"

MARKET_CLOSE_HOUR = 16

# FRED podaje stopy w procentach, zmiany zapisujemy w punktach bazowych
RATE_SERIES = {
    "DGS2": "dgs2_chg_bp",
    "DGS10": "dgs10_chg_bp",
    "T10Y2Y": "t10y2y_chg_bp",
    "DFII10": "dfii10_chg_bp",
    "BAMLH0A0HYM2": "hy_spread_chg_bp",
}

# starsza obserwacja oznacza zaległe dane, lepiej NULL niż sztuczna zmiana 0
MAX_STALENESS = pd.Timedelta(days=5)


def get_tables(engine) -> tuple[Table, Table, Table, Table, Table]:
    raw = MetaData(schema="raw")
    stock_prices = Table("stock_prices", raw, autoload_with=engine)
    fred_observations = Table("fred_observations", raw, autoload_with=engine)
    finnhub_news = Table("finnhub_news", raw, autoload_with=engine)
    news_sentiment = Table("news_sentiment", MetaData(schema="features"), autoload_with=engine)
    daily_features = Table("daily_features", MetaData(schema="marts"), autoload_with=engine)

    return stock_prices, fred_observations, finnhub_news, news_sentiment, daily_features


def price_features(conn, stock_prices: Table) -> tuple[pd.DataFrame, pd.DatetimeIndex]:
    query = (
        select(stock_prices.c.ticker, stock_prices.c.price_date.label("trade_date"),
               stock_prices.c.adj_close, stock_prices.c.volume)
        .where(stock_prices.c.ticker.in_([BENCHMARK, *TICKERS]))
        .order_by(stock_prices.c.ticker, stock_prices.c.price_date)
    )

    prices = pd.read_sql(query, conn, parse_dates=["trade_date"])
    prices["adj_close"] = prices["adj_close"].astype(float)
    prices["ret"] = prices.groupby("ticker")["adj_close"].pct_change(fill_method=None)

    benchmark = prices[prices["ticker"] == BENCHMARK].set_index("trade_date")
    if benchmark.empty:
        raise RuntimeError(f"Brak cen {BENCHMARK}, uruchom najpierw krok prices")

    trading_days = pd.DatetimeIndex(benchmark.index)
    df = prices[prices["ticker"].isin(TICKERS)].copy()
    df["spy_ret"] = df["trade_date"].map(benchmark["ret"])
    df["excess_ret"] = df["ret"] - df["spy_ret"]
    df["next_excess_ret"] = df.groupby("ticker")["excess_ret"].shift(-1)

    return df, trading_days


def macro_features(conn, fred_observations: Table, trading_days: pd.DatetimeIndex) -> pd.DataFrame:
    query = select(fred_observations.c.series_id, fred_observations.c.obs_date, fred_observations.c.value)
    obs = pd.read_sql(query, conn, parse_dates=["obs_date"])
    obs["value"] = obs["value"].astype(float)

    # FRED ma inny kalendarz niż giełda (np. Columbus Day), więc bierzemy ostatnią wartość przed każdą sesją
    levels = pd.DataFrame(index=trading_days)

    for series_id, group in obs.groupby("series_id"):
        series = group.set_index("obs_date")["value"].sort_index()
        levels[series_id] = series.reindex(trading_days, method="ffill", tolerance=MAX_STALENESS)

    macro = pd.DataFrame(index=trading_days)

    for series_id, column in RATE_SERIES.items():
        macro[column] = levels[series_id].diff() * 100
    macro["dff"] = levels["DFF"]
    macro["vix"] = levels["VIXCLS"]
    macro["vix_chg"] = levels["VIXCLS"].diff()

    return macro.rename_axis("trade_date").reset_index()


def assign_session(published_at: pd.Series, trading_days: pd.DatetimeIndex) -> pd.Series:
    local = pd.to_datetime(published_at, utc=True).dt.tz_convert(MARKET_TZ)
    after_close = (local.dt.hour >= MARKET_CLOSE_HOUR).astype(int)
    earliest = local.dt.tz_localize(None).dt.normalize() + pd.to_timedelta(after_close, unit="D")
    pos = trading_days.searchsorted(earliest.to_numpy())

    sessions = pd.Series(pd.NaT, index=published_at.index, dtype="datetime64[ns]")
    known = pos < len(trading_days)
    sessions[known] = trading_days[pos[known]]

    return sessions


def news_features(conn, finnhub_news: Table, news_sentiment: Table,
                  trading_days: pd.DatetimeIndex) -> tuple[pd.DataFrame, pd.Series]:
    query = (
        select(finnhub_news.c.ticker, finnhub_news.c.published_at,
               news_sentiment.c.score, news_sentiment.c.label)
        .join(news_sentiment, and_(
            news_sentiment.c.news_id == finnhub_news.c.news_id,
            news_sentiment.c.model == MODEL,
        ))
        .where(finnhub_news.c.ticker.in_(TICKERS))
    )
    news = pd.read_sql(query, conn)
    news["trade_date"] = assign_session(news["published_at"], trading_days)
    news = news.dropna(subset=["trade_date"])
    news["is_negative"] = news["label"] == "negative"

    daily = (
        news.groupby(["ticker", "trade_date"])
        .agg(news_count=("score", "size"), sentiment_mean=("score", "mean"),
             negative_share=("is_negative", "mean"))
        .reset_index()
    )
    coverage_start = news.groupby("ticker")["trade_date"].min()

    return daily, coverage_start


def combine(prices: pd.DataFrame, macro: pd.DataFrame, news: pd.DataFrame,
            coverage_start: pd.Series) -> pd.DataFrame:
    df = (
        prices
        .merge(macro, on="trade_date", how="left")
        .merge(news, on=["ticker", "trade_date"], how="left")
    )
    # przed początkiem historii Finnhub brak newsów to brak danych, a nie zero
    covered = df["trade_date"] >= df["ticker"].map(coverage_start)
    df.loc[covered, "news_count"] = df.loc[covered, "news_count"].fillna(0)
    df["news_count"] = df["news_count"].astype("Int64")

    return df


def to_rows(df: pd.DataFrame, built_at: datetime) -> list[dict]:
    df = df.assign(trade_date=df["trade_date"].dt.date, built_at=built_at)
    df = df.astype(object).where(df.notna(), None)

    return df.to_dict("records")


def run() -> None:
    built_at = datetime.now(timezone.utc)
    engine = get_engine()
    stock_prices, fred_observations, finnhub_news, news_sentiment, daily_features = get_tables(engine)

    with engine.connect() as conn:
        prices, trading_days = price_features(conn, stock_prices)
        macro = macro_features(conn, fred_observations, trading_days)
        news, coverage_start = news_features(conn, finnhub_news, news_sentiment, trading_days)

    df = combine(prices, macro, news, coverage_start)
    columns = [c.name for c in daily_features.columns if c.name != "built_at"]
    rows = to_rows(df[columns], built_at)

    with engine.begin() as conn:
        conn.execute(delete(daily_features))
        conn.execute(daily_features.insert(), rows)

    log.info("Zbudowano %d wierszy dla %d spółek, sesje %s – %s",
             len(rows), df["ticker"].nunique(), df["trade_date"].min().date(), df["trade_date"].max().date())