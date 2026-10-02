import os
from fredapi import Fred
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("FRED_API_KEY")
if not api_key:
    raise ValueError("FRED_API_KEY is not set in .env")

# Inicjalizacja z darmowym kluczem
fred = Fred(api_key=api_key)

# 1. Główna stopa procentowa Fed (Federal Funds Effective Rate)
fed_rate = fred.get_series("FEDFUNDS")

# 2. Rentowność 10-letnich obligacji skarbowych (10-Year Treasury Yield)
treasury_10y = fred.get_series("DGS10")

# 3. Rentowność 2-letnich obligacji (do wyliczenia spreadu inwersji krzywej)
treasury_2y = fred.get_series("DGS2")

print(treasury_10y.tail())

