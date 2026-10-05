import argparse
import logging

from extract.interest_rate_api import run as run_fred
from extract.news import run as run_news
from extract.price import run as run_prices
from transform.news_sentiment import run as run_news_sentiment
from transform.daily_features import run as run_daily_features

log = logging.getLogger(__name__)

STEPS = {
    "prices": run_prices,
    "fred": run_fred,
    "news": run_news,
    "sentiment": run_news_sentiment,
    "features": run_daily_features,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Dzienny pipeline danych rynkowych")
    parser.add_argument("steps", nargs="*", help=f"kroki do uruchomienia, domyślnie wszystkie: {', '.join(STEPS)}")
    args = parser.parse_args()

    unknown = [step for step in args.steps if step not in STEPS]
    if unknown:
        parser.error(f"nieznane kroki: {unknown}, dostępne: {list(STEPS)}")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
    failed = []

    for name in args.steps or STEPS:
        log.info("=== %s ===", name)
        try:
            STEPS[name]()
        except Exception:
            log.exception("Krok %s zakończył się błędem", name)
            failed.append(name)

    if failed:
        raise SystemExit(f"Nieudane kroki: {failed}")


if __name__ == "__main__":
    main()