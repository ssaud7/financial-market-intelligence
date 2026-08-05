"""Chunking must preserve boundaries, overlap, and provenance.

A chunk that starts mid-sentence retrieves poorly and reads badly when quoted
back as a citation, and a chunk that has lost its Item number cannot be cited at
all — so both properties are pinned here.
"""

from __future__ import annotations

from pathlib import Path

from fmis.rag.chunker import Chunk, chunk_section
from fmis.rag.loader import Filing, FilingSection

FILING = Filing(
    source_path=Path("aapl-10k-2021.txt"),
    company="APPLE INC",
    cik="0000320193",
    ticker="AAPL",
    form_type="10-K",
    fiscal_year="2021",
    filed_date="20211029",
)

PARAGRAPH = (
    "The Company's business can be impacted by global supply chain disruption. "
    "Component shortages have affected production volumes in prior periods. "
    "Single-source suppliers create concentration risk the Company cannot fully mitigate. "
)


def make_section(repeats: int = 30) -> FilingSection:
    return FilingSection(
        item="Item 1A",
        title="Risk Factors",
        text="\n\n".join(PARAGRAPH for _ in range(repeats)),
    )


def test_chunks_carry_full_provenance():
    chunks = chunk_section(FILING, make_section())
    assert chunks
    for chunk in chunks:
        assert chunk.ticker == "AAPL"
        assert chunk.fiscal_year == "2021"
        assert chunk.item == "Item 1A"
        assert chunk.section_title == "Risk Factors"
        assert chunk.source_file == "aapl-10k-2021.txt"


def test_citation_label_is_human_readable():
    chunk = chunk_section(FILING, make_section())[0]
    assert chunk.citation_label == "AAPL 10-K 2021 · Item 1A Risk Factors"


def test_chunks_respect_the_size_budget():
    size = 1600
    for chunk in chunk_section(FILING, make_section(), size=size, overlap=250):
        # The splitter may run slightly past the target to reach a clean
        # boundary, but must not run away.
        assert len(chunk.text) <= size * 1.3


def test_consecutive_chunks_overlap():
    chunks = chunk_section(FILING, make_section(), size=1200, overlap=300)
    assert len(chunks) >= 2
    for earlier, later in zip(chunks, chunks[1:]):
        assert later.char_start < earlier.char_end, "chunks must overlap, not abut"


def test_chunk_ids_are_stable_across_runs():
    """Content-addressed ids let re-indexing upsert instead of duplicating."""
    first = chunk_section(FILING, make_section())
    second = chunk_section(FILING, make_section())
    assert [c.chunk_id for c in first] == [c.chunk_id for c in second]


def test_chunk_ids_are_unique_within_a_section():
    chunks = chunk_section(FILING, make_section())
    assert len({c.chunk_id for c in chunks}) == len(chunks)


def test_tiny_sections_produce_no_fragments():
    section = FilingSection(item="Item 3", title="Legal Proceedings", text="None.")
    assert chunk_section(FILING, section) == []


def test_metadata_is_scalar_only():
    """Chroma rejects nested metadata values."""
    chunk: Chunk = chunk_section(FILING, make_section())[0]
    for key, value in chunk.as_metadata().items():
        assert isinstance(value, (str, int, float, bool)), f"{key} is {type(value)}"
    assert "text" not in chunk.as_metadata()
