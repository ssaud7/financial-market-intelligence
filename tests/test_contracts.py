"""The data contract must reject every fault the producer knows how to inject.

These run without Kafka, Spark or a network, so they are the fast guard against
someone loosening the contract and silently reopening the ingestion boundary.
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import pytest
from pydantic import ValidationError

from fmis.ingestion.contracts import RejectionReason, StockQuote, classify_rejection
from fmis.ingestion.corruption import CORRUPTIONS

CLEAN_PAYLOAD = {
    "ticker": "AAPL",
    "trade_date": "2021-06-15",
    "open": 129.5,
    "high": 130.8,
    "low": 128.9,
    "close": 130.1,
    "adj_close": 128.4,
    "volume": 72_000_000,
}


def validate(payload: dict | bytes) -> tuple[StockQuote | None, str | None]:
    """Mirror of the consumer's decision path, minus the Kafka plumbing."""
    if isinstance(payload, bytes):
        try:
            payload = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, RejectionReason.MALFORMED_JSON.value
    try:
        return StockQuote.model_validate(payload), None
    except ValidationError as exc:
        reason, _ = classify_rejection(exc)
        return None, reason.value


def test_clean_payload_is_admitted():
    quote, reason = validate(dict(CLEAN_PAYLOAD))
    assert reason is None
    assert quote is not None
    assert quote.ticker == "AAPL"
    assert quote.trade_date == date(2021, 6, 15)


@pytest.mark.parametrize("corruption", CORRUPTIONS, ids=lambda c: c.name)
def test_every_injected_fault_is_rejected_with_the_expected_reason(corruption):
    quote, reason = validate(corruption.apply(dict(CLEAN_PAYLOAD)))
    assert quote is None, f"{corruption.name} ({corruption.description}) was wrongly admitted"
    assert reason == corruption.expected_reason.value


def test_ticker_is_normalised_to_upper_case():
    quote, _ = validate({**CLEAN_PAYLOAD, "ticker": " msft "})
    assert quote is not None and quote.ticker == "MSFT"


def test_class_share_tickers_are_valid():
    for symbol in ("BRK.B", "BF-B"):
        quote, reason = validate({**CLEAN_PAYLOAD, "ticker": symbol})
        assert quote is not None, f"{symbol} should be a valid ticker, rejected as {reason}"


@pytest.mark.parametrize(
    ("field_overrides", "expected"),
    [
        ({"high": 100.0, "low": 200.0}, RejectionReason.OHLC_INCONSISTENT),
        # high must bracket close
        ({"high": 129.0, "close": 130.1, "low": 128.0}, RejectionReason.OHLC_INCONSISTENT),
        # low must bracket open
        ({"low": 130.0, "open": 129.5, "high": 131.0}, RejectionReason.OHLC_INCONSISTENT),
    ],
)
def test_cross_field_ohlc_rules(field_overrides, expected):
    quote, reason = validate({**CLEAN_PAYLOAD, **field_overrides})
    assert quote is None
    assert reason == expected.value


def test_zero_volume_is_allowed_but_negative_is_not():
    quote, _ = validate({**CLEAN_PAYLOAD, "volume": 0})
    assert quote is not None, "a halted session legitimately trades zero shares"

    quote, reason = validate({**CLEAN_PAYLOAD, "volume": -1})
    assert quote is None and reason == RejectionReason.NEGATIVE_VOLUME.value


def test_future_dated_session_is_rejected():
    future = (date.today() + timedelta(days=1)).isoformat()
    quote, reason = validate({**CLEAN_PAYLOAD, "trade_date": future})
    assert quote is None and reason == RejectionReason.FUTURE_DATED.value


def test_adj_close_is_optional():
    payload = {k: v for k, v in CLEAN_PAYLOAD.items() if k != "adj_close"}
    quote, _ = validate(payload)
    assert quote is not None and quote.adj_close is None


def test_bronze_record_is_json_serialisable():
    quote, _ = validate(dict(CLEAN_PAYLOAD))
    assert quote is not None
    encoded = json.dumps(quote.to_bronze_record())
    assert json.loads(encoded)["trade_date"] == "2021-06-15"


def test_contract_is_immutable():
    quote, _ = validate(dict(CLEAN_PAYLOAD))
    assert quote is not None
    with pytest.raises(ValidationError):
        quote.close = 999.0  # type: ignore[misc]
