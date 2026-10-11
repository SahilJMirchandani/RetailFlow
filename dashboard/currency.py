"""Display-only FX conversions used by RetailFlow dashboards.

Rates are pinned historical reference/market observations for reproducible
visualisation. They are not live quotes and must not be used for accounting.
Converted values are never written back to the source datasets.
"""

from math import isfinite

# DummyJSON sample product price -> INR. FBIL reference rate as published
# on NSE India for 8 October 2026.
USD_TO_INR_RATE = 96.7733
USD_TO_INR_RATE_AS_OF = "8 October 2026"
USD_TO_INR_SOURCE_URL = (
    "https://www.nseindia.com/report-detail/rbi-reference-rate-statistics"
)

# Olist Brazilian e-commerce monetary values (BRL) -> INR. Historical
# BRL/INR market observation for 9 October 2026.
BRL_TO_INR_RATE = 19.3500
BRL_TO_INR_RATE_AS_OF = "9 October 2026"
BRL_TO_INR_SOURCE_URL = (
    "https://www.exchangerates.org.uk/BRL-INR-exchange-rate-history.html"
)


def _convert_to_inr(value, rate):
    """Convert a finite numeric value using a fixed display rate."""

    try:
        amount = float(value)
    except (TypeError, ValueError):
        return None

    if not isfinite(amount):
        return None

    return amount * rate


def convert_usd_to_inr(value):
    """Convert a finite numeric USD amount to INR, or return None if invalid."""

    return _convert_to_inr(value, USD_TO_INR_RATE)


def convert_brl_to_inr(value):
    """Convert a finite numeric Brazilian-real amount to INR for display."""

    return _convert_to_inr(value, BRL_TO_INR_RATE)
