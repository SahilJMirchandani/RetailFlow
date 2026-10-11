"""Display-only currency conversion settings for the RetailFlow product gallery.

The DummyJSON gallery assumes sample product prices are denominated in USD.
This fixed FBIL reference rate is for presentation only; it is not live and
must not be used for accounting or written back to the source catalogue.
"""

from math import isfinite

USD_TO_INR_RATE = 96.7733
USD_TO_INR_RATE_AS_OF = "8 October 2026"
USD_TO_INR_SOURCE_URL = (
    "https://www.nseindia.com/report-detail/rbi-reference-rate-statistics"
)


def convert_usd_to_inr(value):
    """Convert a finite numeric USD amount to INR, or return None if invalid."""

    try:
        amount = float(value)
    except (TypeError, ValueError):
        return None

    if not isfinite(amount):
        return None

    return amount * USD_TO_INR_RATE
