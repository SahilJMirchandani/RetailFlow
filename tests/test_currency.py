"""Tests for display-only USD/BRL to INR currency conversions."""

import math

import pytest

from dashboard.currency import (
    BRL_TO_INR_RATE,
    BRL_TO_INR_RATE_AS_OF,
    USD_TO_INR_RATE,
    convert_brl_to_inr,
    convert_usd_to_inr,
    format_brl_as_inr,
    format_brl_as_inr_compact,
)


def test_usd_to_inr_uses_documented_fixed_rate():
    assert USD_TO_INR_RATE == pytest.approx(96.7733)
    assert convert_usd_to_inr(10) == pytest.approx(967.733)


@pytest.mark.parametrize("value", [None, "not-a-price", float("nan"), math.inf])
def test_invalid_or_nonfinite_usd_price_returns_none(value):
    assert convert_usd_to_inr(value) is None


def test_brl_to_inr_uses_documented_fixed_rate():
    assert BRL_TO_INR_RATE == pytest.approx(17.4612)
    assert BRL_TO_INR_RATE_AS_OF == "2 November 2026"
    assert convert_brl_to_inr(100) == pytest.approx(1746.12)


@pytest.mark.parametrize("value", [None, "invalid", float("nan"), math.inf])
def test_invalid_or_nonfinite_brl_amount_returns_none(value):
    assert convert_brl_to_inr(value) is None


def test_brl_to_inr_full_formatter_uses_indian_grouping_and_paise():
    assert format_brl_as_inr(13_591_643.70) == "₹23,73,26,408.97"
    assert format_brl_as_inr(100) == "₹1,746.12"
    assert format_brl_as_inr(-100) == "-₹1,746.12"
    assert format_brl_as_inr(None) == "₹0.00"


def test_brl_to_inr_compact_formatter_uses_crore_and_lakh():
    assert format_brl_as_inr_compact(13_591_643.70) == "₹23.73 Cr"
    assert format_brl_as_inr_compact(2_251_909) == "₹3.93 Cr"
    assert format_brl_as_inr_compact(100_000) == "₹17.46 L"
    assert format_brl_as_inr_compact(100) == "₹1,746.12"
