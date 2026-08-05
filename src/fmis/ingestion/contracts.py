"""The ingestion data contract (rubric deliverable 1).

``StockQuote`` is the schema every message must satisfy to be admitted to the
lakehouse. It is enforced at the ingestion boundary — inside the Kafka consumer,
before a single byte reaches Bronze — so malformed data can never contaminate a
downstream layer.

The contract encodes three classes of rule:

* **Structural** — required fields, parseable types (a corrupted timestamp such
  as ``2021-13-45`` fails here).
* **Domain** — prices must be strictly positive, volume non-negative, tickers
  must look like tickers.
* **Cross-field** — the OHLC relationships must hold: ``high`` is the session
  maximum and ``low`` the session minimum, so ``high >= low`` and both bracket
  ``open`` and ``close``.

When validation fails, :func:`classify_rejection` turns Pydantic's error list
into a stable, greppable reason code that is recorded with the quarantined
record — the rubric requires the rejection reason, not just the rejection.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

# A plausible ticker: 1-6 uppercase letters, optionally with a class suffix
# such as BRK.B or BF-B. Anything else is a corrupted or missing symbol.
TICKER_PATTERN = r"^[A-Z]{1,6}([.\-][A-Z]{1,2})?$"


class RejectionReason(StrEnum):
    """Stable codes recorded against every quarantined record."""

    MALFORMED_JSON = "MALFORMED_JSON"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_TICKER = "INVALID_TICKER"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    NON_POSITIVE_PRICE = "NON_POSITIVE_PRICE"
    NEGATIVE_VOLUME = "NEGATIVE_VOLUME"
    OHLC_INCONSISTENT = "OHLC_INCONSISTENT"
    WRONG_TYPE = "WRONG_TYPE"
    FUTURE_DATED = "FUTURE_DATED"
    UNKNOWN = "UNKNOWN"


class StockQuote(BaseModel):
    """One company's daily trading record, as admitted to the lakehouse."""

    model_config = ConfigDict(
        extra="forbid",           # unexpected fields are a contract breach, not a curiosity
        str_strip_whitespace=True,
        frozen=True,
    )

    ticker: str = Field(..., pattern=TICKER_PATTERN, description="Business key")
    trade_date: date = Field(..., description="Session date (UTC calendar date)")
    open: float = Field(..., gt=0, description="Opening price")
    high: float = Field(..., gt=0, description="Session high")
    low: float = Field(..., gt=0, description="Session low")
    close: float = Field(..., gt=0, description="Closing price")
    adj_close: float | None = Field(default=None, gt=0, description="Split/dividend adjusted close")
    volume: int = Field(..., ge=0, description="Shares traded")

    # Provenance, stamped by the producer rather than supplied by the source.
    source: str = Field(default="sp500-daily-csv")
    event_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("ticker", mode="before")
    @classmethod
    def _upper(cls, value: Any) -> Any:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("trade_date")
    @classmethod
    def _not_in_the_future(cls, value: date) -> date:
        # A daily bar dated in the future is a clock or parsing fault upstream.
        if value > datetime.now(timezone.utc).date():
            raise ValueError("trade_date is in the future")
        return value

    @model_validator(mode="after")
    def _ohlc_consistent(self) -> "StockQuote":
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) is below low ({self.low})")
        if self.high < max(self.open, self.close):
            raise ValueError(
                f"high ({self.high}) is below open/close max ({max(self.open, self.close)})"
            )
        if self.low > min(self.open, self.close):
            raise ValueError(
                f"low ({self.low}) is above open/close min ({min(self.open, self.close)})"
            )
        return self

    def to_bronze_record(self) -> dict[str, Any]:
        """Serialise for the Bronze landing zone (JSON-safe types only)."""
        return {
            "ticker": self.ticker,
            "trade_date": self.trade_date.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "adj_close": self.adj_close,
            "volume": self.volume,
            "source": self.source,
            "event_ts": self.event_ts.isoformat(),
        }


# --------------------------------------------------------------------------
# Rejection classification
# --------------------------------------------------------------------------

_PRICE_FIELDS = {"open", "high", "low", "close", "adj_close"}


def _classify_one(err: dict[str, Any]) -> RejectionReason:
    loc = err.get("loc") or ()
    field = str(loc[0]) if loc else ""
    err_type = str(err.get("type", ""))
    message = str(err.get("msg", "")).lower()

    if err_type == "missing":
        return RejectionReason.MISSING_FIELD
    if err_type == "extra_forbidden":
        return RejectionReason.WRONG_TYPE
    if field == "ticker":
        return RejectionReason.INVALID_TICKER
    if field == "trade_date":
        if "future" in message:
            return RejectionReason.FUTURE_DATED
        return RejectionReason.INVALID_TIMESTAMP
    if field == "volume":
        # ge=0 breach is a negative volume; anything else is a type problem.
        return (
            RejectionReason.NEGATIVE_VOLUME
            if "greater than or equal" in message
            else RejectionReason.WRONG_TYPE
        )
    if field in _PRICE_FIELDS:
        return (
            RejectionReason.NON_POSITIVE_PRICE
            if "greater than" in message
            else RejectionReason.WRONG_TYPE
        )
    # Model-level validators report against the whole object.
    if err_type in {"value_error", "assertion_error"} and not field:
        return RejectionReason.OHLC_INCONSISTENT
    if "high" in message and "low" in message:
        return RejectionReason.OHLC_INCONSISTENT
    return RejectionReason.UNKNOWN


def classify_rejection(exc: ValidationError) -> tuple[RejectionReason, list[dict[str, Any]]]:
    """Reduce a Pydantic ``ValidationError`` to one primary reason plus detail.

    Returns the reason code for the first (highest-priority) violation and a
    JSON-serialisable list of every violation, both of which are written to the
    quarantine record and the dead-letter message.
    """
    details: list[dict[str, Any]] = []
    reasons: list[RejectionReason] = []

    for err in exc.errors():
        reason = _classify_one(err)
        reasons.append(reason)
        details.append(
            {
                "field": ".".join(str(part) for part in (err.get("loc") or ())) or "<record>",
                "reason": reason.value,
                "error_type": err.get("type"),
                "message": err.get("msg"),
                # repr keeps the offending value visible without risking a
                # non-serialisable object landing in the quarantine file.
                "received": repr(err.get("input"))[:200],
            }
        )

    primary = reasons[0] if reasons else RejectionReason.UNKNOWN
    return primary, details
