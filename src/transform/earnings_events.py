import logging
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import MetaData, Table, delete, select
from transform.daily_features import MARKET_CLOSE_HOUR, MARKET_TZ, assign_session
from utils.db import get_engine

log = logging.getLogger(__name__)

MARKET_OPEN = pd.Timedelta(hours=9, minutes=30)

# sesje względem dnia reakcji (0), obie granice włącznie
WINDOWS = {
    "car_pre": (-5, -1),
    "ret_day0": (0, 0),
    "car_0_1": (0, 1),
    "car_drift": (2, 20),
}


def get_tables(engine) -> tuple[Table, Table, Table]:
    earnings = Table("earnings", MetaData(schema="raw"), autoload_with=engine)
    marts = MetaData(schema="marts")
    daily_features = Table("daily_features", marts, autoload_with=engine)
    earnings_events = Table("earnings_events", marts, autoload_with=engine)

    return earnings, daily_features, earnings_events


def load_returns(conn, daily_features: Table) -> pd.DataFrame:
    query = select(daily_features.c.ticker, daily_features.c.trade_date, daily_features.c.excess_ret)
    returns = pd.read_sql(query, conn, parse_dates=["trade_date"])
    if returns.empty:
        raise RuntimeError("Brak danych w marts.daily_features, uruchom najpierw krok features")

    return returns.sort_values(["ticker", "trade_date"], ignore_index=True)


def load_earnings(conn, earnings: Table) -> pd.DataFrame:
    query = select(earnings.c.ticker, earnings.c.announced_at, earnings.c.eps_estimate,
                   earnings.c.eps_reported, earnings.c.surprise_pct)
    df = pd.read_sql(query, conn)
    df["announced_at"] = pd.to_datetime(df["announced_at"], utc=True)

    for column in ["eps_estimate", "eps_reported", "surprise_pct"]:
        df[column] = df[column].astype(float)

    return df


def announce_timing(announced_at: pd.Series) -> pd.Series:
    local = announced_at.dt.tz_convert(MARKET_TZ)
    time_of_day = local - local.dt.normalize()

    timing = pd.Series("during_market", index=announced_at.index)
    timing[time_of_day < MARKET_OPEN] = "before_open"
    timing[local.dt.hour >= MARKET_CLOSE_HOUR] = "after_close"
    # Yahoo podaje północ, gdy nie zna godziny publikacji
    timing[time_of_day == pd.Timedelta(0)] = "unknown"

    return timing


def window_returns(returns: pd.DataFrame) -> pd.DataFrame:
    df = returns.copy()
    grouped = df.groupby("ticker")["excess_ret"]

    for column, (start, end) in WINDOWS.items():
        length = end - start + 1
        # rolling kończy okno w bieżącej sesji, więc przesuwamy je o end; niepełne okno daje NULL
        df[column] = grouped.transform(lambda x, n=length, e=end: x.rolling(n).sum().shift(-e))

    return df.drop(columns="excess_ret")


def build_events(earnings: pd.DataFrame, returns: pd.DataFrame) -> pd.DataFrame:
    trading_days = pd.DatetimeIndex(returns["trade_date"].unique()).sort_values()

    # Yahoo sięga do 2014, a searchsorted przypisałby starsze wyniki do pierwszej sesji
    first_session = trading_days[0].tz_localize(MARKET_TZ)
    events = earnings[earnings["announced_at"] >= first_session].copy()

    events["timing"] = announce_timing(events["announced_at"])
    events["reaction_date"] = assign_session(events["announced_at"], trading_days)
    events = events.dropna(subset=["reaction_date"])

    events["eps_surprise"] = events["eps_reported"] - events["eps_estimate"]
    # procent z Yahoo wybucha przy szacunku bliskim zera, ranga w obrębie spółki jest odporna na skalę
    events["surprise_rank"] = events.groupby("ticker")["surprise_pct"].rank(pct=True)

    return (
        events
        .merge(window_returns(returns), left_on=["ticker", "reaction_date"],
               right_on=["ticker", "trade_date"], how="left")
        .drop(columns="trade_date")
    )


def to_rows(df: pd.DataFrame, built_at: datetime) -> list[dict]:
    df = df.assign(reaction_date=df["reaction_date"].dt.date, built_at=built_at)
    df = df.astype(object).where(df.notna(), None)

    return df.to_dict("records")


def run() -> None:
    built_at = datetime.now(timezone.utc)
    engine = get_engine()
    earnings, daily_features, earnings_events = get_tables(engine)

    with engine.connect() as conn:
        returns = load_returns(conn, daily_features)
        reported = load_earnings(conn, earnings)

    events = build_events(reported, returns)
    columns = [c.name for c in earnings_events.columns if c.name != "built_at"]
    rows = to_rows(events[columns], built_at)

    with engine.begin() as conn:
        conn.execute(delete(earnings_events))
        if rows:
            conn.execute(earnings_events.insert(), rows)

    log.info("Zbudowano %d zdarzeń dla %d spółek, %d z pełnym oknem dryfu",
             len(rows), events["ticker"].nunique(), events["car_drift"].notna().sum())