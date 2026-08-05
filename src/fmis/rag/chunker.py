"""Splitting filing sections into retrievable chunks.

Chunking is where most of a RAG system's answer quality is won or lost, so this
is deliberately not a naive fixed-width split:

* **Boundaries are respected.** The splitter prefers to break on a blank line,
  then a sentence end, and only cuts mid-sentence as a last resort. A chunk that
  starts halfway through a risk-factor sentence retrieves badly and reads worse
  when quoted back as a citation.
* **Chunks overlap.** A claim that straddles a boundary would otherwise be
  retrievable from neither side.
* **Every chunk keeps its provenance.** Company, ticker, fiscal year, Item
  number, section title and character offsets travel with the text, which is
  what lets the generator produce a citation a reader can verify in the source
  filing.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable

from fmis.config import settings
from fmis.logging_setup import configure_logging
from fmis.rag.loader import Filing, FilingSection

log = configure_logging("rag_chunker")

_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


@dataclass
class Chunk:
    """One retrievable passage plus everything needed to cite it."""

    chunk_id: str
    text: str
    company: str
    ticker: str
    fiscal_year: str
    form_type: str
    item: str
    section_title: str
    source_file: str
    char_start: int
    char_end: int

    @property
    def citation_label(self) -> str:
        """Human-readable source label, e.g. ``AAPL 10-K 2021 · Item 1A``."""
        who = self.ticker or self.company
        year = self.fiscal_year or "n.d."
        return f"{who} {self.form_type} {year} · {self.item} {self.section_title}"

    def as_metadata(self) -> dict[str, Any]:
        """Chroma metadata values must be scalars, so everything is flattened."""
        payload = asdict(self)
        payload.pop("text")
        payload["citation_label"] = self.citation_label
        return payload


def _split_with_overlap(text: str, size: int, overlap: int) -> list[tuple[int, int]]:
    """Return (start, end) spans that tile ``text``, preferring clean breaks."""
    spans: list[tuple[int, int]] = []
    length = len(text)
    start = 0

    while start < length:
        end = min(start + size, length)

        if end < length:
            window_start = start + int(size * 0.6)
            window = text[window_start:end]

            # Prefer a paragraph break, then a sentence end, then give up and
            # cut at the hard limit.
            para = window.rfind("\n\n")
            if para != -1:
                end = window_start + para + 2
            else:
                sentences = list(_SENTENCE_END.finditer(window))
                if sentences:
                    end = window_start + sentences[-1].end()

        spans.append((start, end))

        if end >= length:
            break
        start = max(end - overlap, start + 1)

    return spans


def chunk_section(
    filing: Filing,
    section: FilingSection,
    *,
    size: int | None = None,
    overlap: int | None = None,
) -> list[Chunk]:
    size = size or settings.rag_chunk_chars
    overlap = overlap or settings.rag_chunk_overlap_chars

    chunks: list[Chunk] = []
    for index, (start, end) in enumerate(_split_with_overlap(section.text, size, overlap)):
        body = section.text[start:end].strip()
        # Skip fragments too short to carry a claim.
        if len(body) < 200:
            continue

        # Content-addressed id: re-indexing identical text yields the same id,
        # so the vector store upserts rather than duplicating.
        digest = hashlib.sha1(
            f"{filing.source_path.name}|{section.item}|{index}|{body[:200]}".encode()
        ).hexdigest()[:16]

        chunks.append(
            Chunk(
                chunk_id=digest,
                text=body,
                company=filing.company,
                ticker=filing.ticker or "",
                fiscal_year=filing.fiscal_year or "",
                form_type=filing.form_type,
                item=section.item,
                section_title=section.title,
                source_file=filing.source_path.name,
                char_start=start,
                char_end=end,
            )
        )

    return chunks


def chunk_filings(filings: Iterable[Filing]) -> list[Chunk]:
    chunks: list[Chunk] = []
    for filing in filings:
        for section in filing.sections:
            chunks.extend(chunk_section(filing, section))

    if not chunks:
        raise ValueError("Chunking produced no passages — check the filing parser output.")

    by_item: dict[str, int] = {}
    for chunk in chunks:
        by_item[chunk.item] = by_item.get(chunk.item, 0) + 1

    log.info(
        "rag.chunked",
        chunks=len(chunks),
        filings=len({c.source_file for c in chunks}),
        per_item=dict(sorted(by_item.items())),
        mean_chars=round(sum(len(c.text) for c in chunks) / len(chunks)),
    )
    return chunks
