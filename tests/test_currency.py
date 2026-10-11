"""Tests for display-only USD to INR conversion."""

import math

import pytest

from dashboard.currency import (
    USD_TO_INR_RATE,
    convert_usd_to_inr,
)


def test_usd_to_inr_uses_documented_fixed_rate():
    assert USD_TO_INR_RATE == pytest.approx(96.7733)
    assert convert_usd_to_inr(10) == pytest.approx(967.733)


@pytest.mark.parametrize("value", [None, "not-a-price", float("nan"), math.inf])
def test_invalid_or_nonfinite_price_returns_none(value):
    assert convert_usd_to_inr(value) is None
