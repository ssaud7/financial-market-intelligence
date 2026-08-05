"""The expectation suite the Silver layer must satisfy before Gold is touched.

The rubric names one check explicitly — ``High >= Low`` — and that is the first
expectation below. The rest close the obvious gaps around it: a session whose
high clears its low but sits below its own close is just as impossible, and a
duplicated ``(ticker, trade_date)`` would silently double-count in the moving
average.

Expectations are declared as data rather than imperatively so the suite can be
rendered into the README and the notebooks, and so a reader can see the full
contract in one screen.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUITE_NAME = "silver_quotes_suite"

PRICE_COLUMNS = ("open", "high", "low", "close")
REQUIRED_COLUMNS = ("ticker", "trade_date", "open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class ExpectationSpec:
    """One expectation: the GE class name, its kwargs, and why it is here."""

    expectation: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    rationale: str = ""


def build_specs(known_tickers: list[str] | None = None) -> list[ExpectationSpec]:
    specs: list[ExpectationSpec] = [
        # ---- The rubric's named check ------------------------------------
        ExpectationSpec(
            "ExpectColumnPairValuesAToBeGreaterThanB",
            {"column_A": "high", "column_B": "low", "or_equal": True},
            "A session's high cannot be below its low. This is the check the "
            "capstone rubric names explicitly.",
        ),
        # ---- The rest of the OHLC relationships --------------------------
        ExpectationSpec(
            "ExpectColumnPairValuesAToBeGreaterThanB",
            {"column_A": "high", "column_B": "close", "or_equal": True},
            "The high must bracket the close, or one of them is wrong.",
        ),
        ExpectationSpec(
            "ExpectColumnPairValuesAToBeGreaterThanB",
            {"column_A": "close", "column_B": "low", "or_equal": True},
            "The low must bracket the close.",
        ),
        ExpectationSpec(
            "ExpectColumnPairValuesAToBeGreaterThanB",
            {"column_A": "high", "column_B": "open", "or_equal": True},
            "The high must bracket the open.",
        ),
        ExpectationSpec(
            "ExpectColumnPairValuesAToBeGreaterThanB",
            {"column_A": "open", "column_B": "low", "or_equal": True},
            "The low must bracket the open.",
        ),
        # ---- Completeness -------------------------------------------------
        *[
            ExpectationSpec(
                "ExpectColumnValuesToNotBeNull",
                {"column": column},
                f"{column} is required downstream; a null would propagate into the aggregate.",
            )
            for column in REQUIRED_COLUMNS
        ],
        # ---- Domain ranges ------------------------------------------------
        *[
            ExpectationSpec(
                "ExpectColumnValuesToBeBetween",
                {"column": column, "min_value": 0, "strict_min": True},
                f"A traded {column} price of zero or less is not a real quote.",
            )
            for column in PRICE_COLUMNS
        ],
        ExpectationSpec(
            "ExpectColumnValuesToBeBetween",
            {"column": "volume", "min_value": 0},
            "Share volume is a count; it cannot be negative. Zero is legitimate "
            "for a halted session.",
        ),
        # ---- Grain --------------------------------------------------------
        ExpectationSpec(
            "ExpectCompoundColumnsToBeUnique",
            {"column_list": ["ticker", "trade_date"]},
            "Silver's grain is one row per ticker per session. A duplicate would "
            "be double-counted by the 30-session moving average in Gold.",
        ),
        ExpectationSpec(
            "ExpectTableRowCountToBeBetween",
            {"min_value": 1},
            "An empty Silver table means the ingestion stage produced nothing; "
            "merging that into Gold would be worse than failing.",
        ),
    ]

    if known_tickers:
        specs.append(
            ExpectationSpec(
                "ExpectColumnValuesToBeInSet",
                {"column": "ticker", "value_set": sorted(known_tickers)},
                "Only the configured universe should reach Silver; anything else "
                "indicates a mis-routed topic or a stale landing file.",
            )
        )

    return specs


def as_markdown(specs: list[ExpectationSpec]) -> str:
    lines = [
        "| Expectation | Parameters | Why |",
        "| --- | --- | --- |",
    ]
    for spec in specs:
        params = ", ".join(f"`{k}={v}`" for k, v in spec.kwargs.items())
        lines.append(f"| `{spec.expectation}` | {params} | {spec.rationale} |")
    return "\n".join(lines)
