"""Display-only FX conversions used by RetailFlow dashboards.

Rates are pinned historical reference/market observations for reproducible
visualisation. They are not live quotes and must not be used for accounting.
Converted values are never written back to the source datasets.
"""

from math import isfinite
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


# DummyJSON sample product price -> INR. FBIL reference rate as published
# on NSE India for 8 October 2026.
USD_TO_INR_RATE = 96.7733
USD_TO_INR_RATE_AS_OF = "8 October 2026"
USD_TO_INR_SOURCE_URL = (
    "https://www.nseindia.com/report-detail/rbi-reference-rate-statistics"
)

# Olist Brazilian e-commerce monetary values (BRL) -> INR. Historical
# BRL/INR market observation for 2 November 2026.
BRL_TO_INR_RATE = 17.4612
BRL_TO_INR_RATE_AS_OF = "2 November 2026"
BRL_TO_INR_SOURCE_URL = (
    "https://www.mataf.net/en/currency/converter-BRL-INR/data"
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



def format_brl_as_inr(value):
    """Format a source BRL amount as INR using decimal half-up rounding."""

    try:
        amount = Decimal(str(value if value is not None else 0))
    except (InvalidOperation, ValueError, TypeError):
        amount = Decimal("0")

    if not amount.is_finite():
        amount = Decimal("0")

    converted = (
        amount * Decimal(str(BRL_TO_INR_RATE))
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    sign = "-" if converted < 0 else ""
    whole, fractional = f"{abs(converted):.2f}".split(".")
    if len(whole) > 3:
        last_three = whole[-3:]
        prefix = whole[:-3]
        groups = []
        while prefix:
            groups.insert(0, prefix[-2:])
            prefix = prefix[:-2]
        whole = ",".join(groups + [last_three])

    return f"{sign}₹{whole}.{fractional}"

def format_brl_as_inr_compact(value):
    """Format BRL as a compact INR metric (crore/lakh) for dashboard cards."""

    try:
        amount = Decimal(str(value if value is not None else 0))
    except (InvalidOperation, ValueError, TypeError):
        amount = Decimal("0")

    if not amount.is_finite():
        amount = Decimal("0")

    converted = (
        amount * Decimal(str(BRL_TO_INR_RATE))
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    sign = "-" if converted < 0 else ""
    absolute = abs(converted)

    if absolute >= Decimal("10000000"):
        display_value = (
            absolute / Decimal("10000000")
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        suffix = "Cr"
    elif absolute >= Decimal("100000"):
        display_value = (
            absolute / Decimal("100000")
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        suffix = "L"
    else:
        return format_brl_as_inr(value)

    return f"{sign}₹{display_value:,.2f} {suffix}"

