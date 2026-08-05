"""Deliberate payload corruption, used to prove the data contract actually bites.

The rubric asks for the failure path to be demonstrated, not just described. The
producer therefore corrupts a configurable fraction of the stream on its way
out, and tags each corrupted message with a Kafka header naming the fault it
injected and the reason code it *expects* the consumer to record.

That header is written purely so :mod:`fmis.ingestion.report` can score the
contract afterwards — how many injected faults were caught, and whether each was
caught for the right reason. **The consumer never reads it.** Validation is
decided solely by the Pydantic contract applied to the message body, exactly as
it would be for genuinely untrusted input.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable

from fmis.ingestion.contracts import RejectionReason

HEADER_FAULT = "x-fmis-injected-fault"
HEADER_EXPECTED_REASON = "x-fmis-expected-reason"


@dataclass(frozen=True)
class Corruption:
    """One fault the producer knows how to inject."""

    name: str
    expected_reason: RejectionReason
    apply: Callable[[dict[str, Any]], dict[str, Any] | bytes]
    description: str


def _negative_price(record: dict[str, Any]) -> dict[str, Any]:
    record["close"] = -abs(record.get("close") or 1.0)
    return record


def _zero_price(record: dict[str, Any]) -> dict[str, Any]:
    record["open"] = 0.0
    return record


def _missing_ticker(record: dict[str, Any]) -> dict[str, Any]:
    record.pop("ticker", None)
    return record


def _blank_ticker(record: dict[str, Any]) -> dict[str, Any]:
    record["ticker"] = "???"
    return record


def _missing_close(record: dict[str, Any]) -> dict[str, Any]:
    record.pop("close", None)
    return record


def _corrupt_timestamp(record: dict[str, Any]) -> dict[str, Any]:
    record["trade_date"] = random.choice(
        ["2021-13-45", "not-a-date", "31/02/2020", "", "0000-00-00"]
    )
    return record


def _future_date(record: dict[str, Any]) -> dict[str, Any]:
    record["trade_date"] = (date.today() + timedelta(days=45)).isoformat()
    return record


def _negative_volume(record: dict[str, Any]) -> dict[str, Any]:
    record["volume"] = -abs(record.get("volume") or 1000)
    return record


def _high_below_low(record: dict[str, Any]) -> dict[str, Any]:
    high, low = record.get("high"), record.get("low")
    if high is not None and low is not None:
        record["high"], record["low"] = low, high
        # Guarantee a real inversion even on a flat session.
        if record["high"] == record["low"]:
            record["high"] = round(record["low"] * 0.5, 6)
    return record


def _non_numeric_price(record: dict[str, Any]) -> dict[str, Any]:
    record["close"] = "N/A"
    return record


def _unexpected_field(record: dict[str, Any]) -> dict[str, Any]:
    record["settlement_instructions"] = "wire to account 00-1234"
    return record


def _truncated_json(record: dict[str, Any]) -> bytes:
    raw = json.dumps(record).encode("utf-8")
    return raw[: max(8, len(raw) // 2)]


CORRUPTIONS: tuple[Corruption, ...] = (
    Corruption("negative_price", RejectionReason.NON_POSITIVE_PRICE, _negative_price,
               "close price flipped negative"),
    Corruption("zero_price", RejectionReason.NON_POSITIVE_PRICE, _zero_price,
               "open price set to zero"),
    Corruption("missing_ticker", RejectionReason.MISSING_FIELD, _missing_ticker,
               "ticker field removed entirely"),
    Corruption("blank_ticker", RejectionReason.INVALID_TICKER, _blank_ticker,
               "ticker replaced with non-symbol text"),
    Corruption("missing_close", RejectionReason.MISSING_FIELD, _missing_close,
               "close field removed entirely"),
    Corruption("corrupt_timestamp", RejectionReason.INVALID_TIMESTAMP, _corrupt_timestamp,
               "unparseable or impossible trade date"),
    Corruption("future_date", RejectionReason.FUTURE_DATED, _future_date,
               "session dated 45 days in the future"),
    Corruption("negative_volume", RejectionReason.NEGATIVE_VOLUME, _negative_volume,
               "share volume flipped negative"),
    Corruption("high_below_low", RejectionReason.OHLC_INCONSISTENT, _high_below_low,
               "session high pushed below session low"),
    Corruption("non_numeric_price", RejectionReason.WRONG_TYPE, _non_numeric_price,
               "close price replaced with a string"),
    Corruption("unexpected_field", RejectionReason.WRONG_TYPE, _unexpected_field,
               "undeclared field added to the payload"),
    Corruption("truncated_json", RejectionReason.MALFORMED_JSON, _truncated_json,
               "message body cut in half, producing invalid JSON"),
)

CORRUPTION_BY_NAME = {c.name: c for c in CORRUPTIONS}


def pick() -> Corruption:
    """Choose a fault to inject, uniformly across the catalogue.

    Uses the module-level ``random`` state, which the producer seeds once, so a
    given ``--seed`` reproduces the same stream of faults end to end.
    """
    return random.choice(CORRUPTIONS)
