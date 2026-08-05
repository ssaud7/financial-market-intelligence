"""Stage 1 — streaming ingestion with a contract enforced at the boundary.

``producer``   streams the S&P 500 daily history to Kafka, corrupting a
               configurable slice of it on the way out.
``consumer``   validates every message against the ``StockQuote`` contract and
               routes failures to the dead-letter topic and quarantine zone.
``report``     scores the contract against the faults that were injected.
"""

from fmis.ingestion.contracts import RejectionReason, StockQuote, classify_rejection

__all__ = ["RejectionReason", "StockQuote", "classify_rejection"]
