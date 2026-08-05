"""Parsing SEC Form 10-K filings into section-aware documents.

The Kaggle *SEC EDGAR Annual Financial Filings* export ships filings as raw
submission text or HTML, one file per company, with no consistent naming. This
module normalises them:

1. walk ``data/raw`` for filing-shaped files (unpacking any zip first),
2. pull the company, CIK, fiscal period and form type out of the SEC header,
3. strip HTML and boilerplate down to readable text, and
4. split the body into the numbered **Items** a 10-K is built from.

Section awareness is what makes the retrieval answerable. "What supply chain
risks does the company cite?" is a question about *Item 1A, Risk Factors*
specifically, and carrying that label on every chunk lets the answer cite
``AAPL 10-K 2021 · Item 1A`` rather than an anonymous offset into a 200-page
document.
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("rag_loader")

FILING_SUFFIXES = {".txt", ".htm", ".html", ".mda", ".xml"}

# The canonical 10-K item structure. Order matters: the parser walks the
# document and treats each heading it finds as the end of the previous item.
ITEM_PATTERNS: list[tuple[str, str, str]] = [
    ("Item 1", "Business", r"item\s*1\s*[\.\:\-–—]?\s*business"),
    ("Item 1A", "Risk Factors", r"item\s*1a\s*[\.\:\-–—]?\s*risk\s*factors"),
    ("Item 1B", "Unresolved Staff Comments", r"item\s*1b\s*[\.\:\-–—]?\s*unresolved"),
    ("Item 2", "Properties", r"item\s*2\s*[\.\:\-–—]?\s*propert"),
    ("Item 3", "Legal Proceedings", r"item\s*3\s*[\.\:\-–—]?\s*legal\s*proceedings"),
    ("Item 5", "Market for Registrant's Common Equity", r"item\s*5\s*[\.\:\-–—]?\s*market\s*for"),
    ("Item 7", "Management's Discussion and Analysis",
     r"item\s*7\s*[\.\:\-–—]?\s*management'?s?\s*discussion"),
    ("Item 7A", "Quantitative and Qualitative Disclosures About Market Risk",
     r"item\s*7a\s*[\.\:\-–—]?\s*quantitative"),
    ("Item 8", "Financial Statements and Supplementary Data",
     r"item\s*8\s*[\.\:\-–—]?\s*financial\s*statements"),
    ("Item 9A", "Controls and Procedures", r"item\s*9a\s*[\.\:\-–—]?\s*controls"),
]

# Sections worth indexing. Financial statement tables (Item 8) are excluded:
# they are dense numeric tables that chunk badly and answer poorly, and the
# lakehouse already holds the quantitative side of this system.
INDEXABLE_ITEMS = {"Item 1", "Item 1A", "Item 3", "Item 7", "Item 7A"}

_HEADER_FIELDS = {
    "company": r"COMPANY CONFORMED NAME:\s*(.+)",
    "cik": r"CENTRAL INDEX KEY:\s*(\d+)",
    "form_type": r"CONFORMED SUBMISSION TYPE:\s*(.+)",
    "period": r"CONFORMED PERIOD OF REPORT:\s*(\d{8})",
    "filed_date": r"FILED AS OF DATE:\s*(\d{8})",
    "ticker": r"TRADING SYMBOL:\s*(.+)",
}


@dataclass
class FilingSection:
    """One numbered Item extracted from a filing."""

    item: str
    title: str
    text: str


@dataclass
class Filing:
    """A parsed 10-K, with provenance and its indexable sections."""

    source_path: Path
    company: str
    cik: str | None
    ticker: str | None
    form_type: str
    fiscal_year: str | None
    filed_date: str | None
    sections: list[FilingSection] = field(default_factory=list)

    @property
    def display_name(self) -> str:
        label = self.ticker or self.company
        year = self.fiscal_year or "unknown year"
        return f"{label} {self.form_type} {year}"

    def as_metadata(self) -> dict[str, Any]:
        return {
            "company": self.company,
            "ticker": self.ticker or "",
            "cik": self.cik or "",
            "form_type": self.form_type,
            "fiscal_year": self.fiscal_year or "",
            "filed_date": self.filed_date or "",
            "source_file": self.source_path.name,
        }


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------

def unpack_archives(raw_dir: Path | None = None) -> None:
    """Extract any zip under ``data/raw`` so filings inside become visible."""
    raw_dir = raw_dir or settings.data_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for archive in sorted(raw_dir.glob("*.zip")):
        target = raw_dir / "extracted" / archive.stem
        if target.exists() and any(target.iterdir()):
            continue
        target.mkdir(parents=True, exist_ok=True)
        log.info("rag.extracting", archive=archive.name)
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(target)


def discover_filings(raw_dir: Path | None = None, *, limit: int | None = None) -> list[Path]:
    """Return candidate filing files, largest first.

    Size ordering is a cheap proxy for completeness: a full 10-K submission is
    hundreds of kilobytes, while index and cover files are tiny.
    """
    raw_dir = raw_dir or settings.data_root / "raw"
    unpack_archives(raw_dir)

    candidates = [
        path
        for path in raw_dir.rglob("*")
        if path.is_file()
        and path.suffix.lower() in FILING_SUFFIXES
        and path.stat().st_size > 20_000
    ]
    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)

    if limit is not None:
        candidates = candidates[:limit]

    log.info("rag.filings_discovered", count=len(candidates), root=str(raw_dir))
    return candidates


# --------------------------------------------------------------------------
# Text extraction
# --------------------------------------------------------------------------

def _strip_markup(raw: str) -> str:
    """Reduce an SEC submission to readable prose."""
    # Drop the XBRL/graphic payloads that pad modern submissions.
    raw = re.sub(r"<(TYPE|DESCRIPTION)>GRAPHIC.*?</DOCUMENT>", " ", raw, flags=re.S | re.I)
    raw = re.sub(r"<XBRL>.*?</XBRL>", " ", raw, flags=re.S | re.I)

    if "<" in raw and re.search(r"<(html|body|div|table|p)\b", raw, re.I):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw, "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
        raw = soup.get_text(separator="\n")

    raw = raw.replace("\xa0", " ").replace("’", "'").replace("“", '"')
    raw = raw.replace("”", '"').replace("—", "—")
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n\s*\n\s*\n+", "\n\n", raw)
    return raw.strip()


def _parse_header(raw: str) -> dict[str, str | None]:
    header = raw[:8000]
    parsed: dict[str, str | None] = {}
    for key, pattern in _HEADER_FIELDS.items():
        match = re.search(pattern, header, re.I)
        parsed[key] = match.group(1).strip() if match else None
    return parsed


def _split_sections(text: str) -> list[FilingSection]:
    """Locate each Item heading and slice the text between consecutive ones.

    A 10-K names every Item twice — once in the table of contents, once at the
    real section. The last occurrence is taken as the real heading, since the
    table of contents always precedes the body.
    """
    hits: list[tuple[int, str, str]] = []
    for item, title, pattern in ITEM_PATTERNS:
        matches = list(re.finditer(pattern, text, re.I))
        if not matches:
            continue
        hits.append((matches[-1].start(), item, title))

    if not hits:
        return []

    hits.sort()
    sections: list[FilingSection] = []
    for index, (start, item, title) in enumerate(hits):
        end = hits[index + 1][0] if index + 1 < len(hits) else len(text)
        body = text[start:end].strip()
        # Anything under a few hundred characters is a stray cross-reference,
        # not a real section.
        if len(body) < 500:
            continue
        sections.append(FilingSection(item=item, title=title, text=body))

    return sections


def parse_filing(path: Path) -> Filing | None:
    """Parse one file into a :class:`Filing`, or ``None`` if it is not a 10-K."""
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        log.warning("rag.unreadable_filing", path=str(path), error=str(exc))
        return None

    header = _parse_header(raw)
    form_type = (header.get("form_type") or "").upper()

    # Accept 10-K and its variants (10-K405, 10-K/A); reject 10-Q, 8-K, etc.
    if form_type and not form_type.startswith("10-K"):
        log.info("rag.skipped_non_10k", path=path.name, form_type=form_type)
        return None

    text = _strip_markup(raw)
    sections = [s for s in _split_sections(text) if s.item in INDEXABLE_ITEMS]

    if not sections:
        log.warning("rag.no_sections_found", path=path.name, chars=len(text))
        return None

    period = header.get("period")
    filing = Filing(
        source_path=path,
        company=header.get("company") or path.stem.replace("_", " ").title(),
        cik=header.get("cik"),
        ticker=(header.get("ticker") or "").upper() or None,
        form_type=form_type or "10-K",
        fiscal_year=period[:4] if period else None,
        filed_date=header.get("filed_date"),
        sections=sections,
    )

    log.info(
        "rag.filing_parsed",
        company=filing.company,
        fiscal_year=filing.fiscal_year,
        sections=[f"{s.item} ({len(s.text)} chars)" for s in filing.sections],
    )
    return filing


def load_filings(*, limit: int | None = None, raw_dir: Path | None = None) -> list[Filing]:
    """Parse every discoverable 10-K under ``data/raw``."""
    filings: list[Filing] = []
    for path in discover_filings(raw_dir, limit=limit):
        parsed = parse_filing(path)
        if parsed is not None:
            filings.append(parsed)

    if not filings:
        raise FileNotFoundError(
            f"No parseable 10-K filings found under {raw_dir or settings.data_root / 'raw'}. "
            "Place the Kaggle SEC EDGAR download (or raw filings from "
            "https://www.sec.gov/edgar) there and re-run."
        )

    log.info(
        "rag.filings_loaded",
        count=len(filings),
        companies=[f.display_name for f in filings],
    )
    return filings


def iter_sections(filings: Iterable[Filing]):
    for filing in filings:
        for section in filing.sections:
            yield filing, section
