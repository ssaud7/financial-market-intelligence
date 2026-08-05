"""Parsing SEC Form 10-K filings into section-aware documents.

Two source shapes are supported, and the better one is preferred automatically.

**JSON sidecars** (the Kaggle *SEC EDGAR Annual Financial Filings* export). Each
filing ships a ``.json`` companion holding clean metadata and the filing's
**Items already separated** into ``item_1``, ``item_1A``, ``item_7`` and so on.
This is the primary path: the alternative is regex-slicing section boundaries out
of 7 MB of inline-XBRL HTML, where a table of contents repeats every heading and
DFIN's generated markup interleaves tags mid-sentence.

**Raw filings** (``.txt`` or ``.htm`` pulled straight from EDGAR). Parsed by
stripping markup and locating the Item headings directly. Kept so the pipeline
also works against a hand-assembled corpus, as the capstone brief allows.

Section awareness is what makes retrieval answerable. "What supply chain risks
does the company cite?" is a question about *Item 1A, Risk Factors*, and carrying
that label on every chunk lets an answer cite `MICROSOFT CORP 10-K 2021 · Item
1A` rather than an anonymous offset into a 200-page document.
"""

from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from fmis.config import settings
from fmis.logging_setup import configure_logging

log = configure_logging("rag_loader")

RAW_FILING_SUFFIXES = {".txt", ".htm", ".html"}

# Item key -> human title. Only sections worth retrieving over are listed.
#
# Item 8 (financial statements) is deliberately excluded: it is dense numeric
# tables that chunk badly and answer worse, and the lakehouse already holds the
# quantitative side of this system. Item 15 (exhibits) is likewise noise.
INDEXABLE_ITEMS: dict[str, str] = {
    "item_1": "Business",
    "item_1A": "Risk Factors",
    "item_3": "Legal Proceedings",
    "item_7": "Management's Discussion and Analysis",
    "item_7A": "Quantitative and Qualitative Disclosures About Market Risk",
}

# Item key -> display number, e.g. "item_1A" -> "Item 1A".
ITEM_LABELS = {key: "Item " + key.split("_", 1)[1] for key in INDEXABLE_ITEMS}

# A section shorter than this is a cross-reference or a "Not applicable.", not
# prose worth indexing.
MIN_SECTION_CHARS = 500

# Regex fallbacks for raw EDGAR filings that have no sidecar.
ITEM_PATTERNS: list[tuple[str, str, str]] = [
    ("Item 1", "Business", r"item\s*1\s*[\.\:\-–—]?\s*business"),
    ("Item 1A", "Risk Factors", r"item\s*1a\s*[\.\:\-–—]?\s*risk\s*factors"),
    ("Item 2", "Properties", r"item\s*2\s*[\.\:\-–—]?\s*propert"),
    ("Item 3", "Legal Proceedings", r"item\s*3\s*[\.\:\-–—]?\s*legal\s*proceedings"),
    ("Item 7", "Management's Discussion and Analysis",
     r"item\s*7\s*[\.\:\-–—]?\s*management'?s?\s*discussion"),
    ("Item 7A", "Quantitative and Qualitative Disclosures About Market Risk",
     r"item\s*7a\s*[\.\:\-–—]?\s*quantitative"),
    ("Item 8", "Financial Statements and Supplementary Data",
     r"item\s*8\s*[\.\:\-–—]?\s*financial\s*statements"),
]
RAW_INDEXABLE = {"Item 1", "Item 1A", "Item 3", "Item 7", "Item 7A"}

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
        return f"{label} {self.form_type} {self.fiscal_year or 'n.d.'}"

    @property
    def indexable_chars(self) -> int:
        return sum(len(s.text) for s in self.sections)

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
        if target.exists() and any(target.rglob("*")):
            continue
        target.mkdir(parents=True, exist_ok=True)
        log.info("rag.extracting", archive=archive.name)
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(target)


# --------------------------------------------------------------------------
# Primary path: JSON sidecars with pre-separated Items
# --------------------------------------------------------------------------

def parse_sidecar(path: Path) -> Filing | None:
    """Build a :class:`Filing` from a Kaggle JSON sidecar."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("rag.unreadable_sidecar", path=str(path), error=str(exc))
        return None

    if not isinstance(payload, dict):
        return None
    # A sidecar for something other than a filing (an index, a manifest) has
    # none of the Item keys.
    if not any(key in payload for key in INDEXABLE_ITEMS):
        return None

    form_type = str(payload.get("filing_type") or "10-K").upper()
    if not form_type.startswith("10-K"):
        return None

    sections: list[FilingSection] = []
    for key, title in INDEXABLE_ITEMS.items():
        body = payload.get(key)
        if not isinstance(body, str):
            continue
        body = body.strip()
        if len(body) < MIN_SECTION_CHARS:
            continue
        sections.append(FilingSection(item=ITEM_LABELS[key], title=title, text=body))

    if not sections:
        return None

    period = str(payload.get("period_of_report") or "")
    return Filing(
        source_path=Path(str(payload.get("filename") or path.name)),
        company=str(payload.get("company") or path.stem).strip(),
        cik=str(payload.get("cik")) if payload.get("cik") is not None else None,
        # The export carries no trading symbol; citations fall back to the
        # company name, which is what the filing itself is titled with anyway.
        ticker=None,
        form_type=form_type,
        fiscal_year=period[:4] if period else None,
        filed_date=str(payload.get("filing_date") or "") or None,
        sections=sections,
    )


def discover_sidecars(raw_dir: Path | None = None) -> list[Path]:
    raw_dir = raw_dir or settings.data_root / "raw"
    unpack_archives(raw_dir)
    return sorted(raw_dir.rglob("*.json"))


# --------------------------------------------------------------------------
# Fallback path: raw EDGAR filings
# --------------------------------------------------------------------------

def _strip_markup(raw: str) -> str:
    """Reduce a raw SEC submission to readable prose."""
    raw = re.sub(r"<(TYPE|DESCRIPTION)>GRAPHIC.*?</DOCUMENT>", " ", raw, flags=re.S | re.I)
    raw = re.sub(r"<XBRL>.*?</XBRL>", " ", raw, flags=re.S | re.I)

    if "<" in raw and re.search(r"<(html|body|div|table|p)\b", raw, re.I):
        import warnings

        from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

        # Inline-XBRL filings open with an XML declaration; parsing them as HTML
        # is intentional and correct here, so silence the advisory warning.
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

        soup = BeautifulSoup(raw, "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
        raw = soup.get_text(separator="\n")

    raw = raw.replace("\xa0", " ").replace("’", "'")
    raw = raw.replace("“", '"').replace("”", '"')
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n\s*\n\s*\n+", "\n\n", raw)
    return raw.strip()


def _split_sections(text: str) -> list[FilingSection]:
    """Slice text between consecutive Item headings.

    A 10-K names every Item at least twice — once in the table of contents, once
    at the real section. The *last* occurrence is taken, since the contents page
    always precedes the body.
    """
    hits: list[tuple[int, str, str]] = []
    for item, title, pattern in ITEM_PATTERNS:
        matches = list(re.finditer(pattern, text, re.I))
        if matches:
            hits.append((matches[-1].start(), item, title))

    if not hits:
        return []

    hits.sort()
    sections: list[FilingSection] = []
    for index, (start, item, title) in enumerate(hits):
        end = hits[index + 1][0] if index + 1 < len(hits) else len(text)
        body = text[start:end].strip()
        if len(body) < MIN_SECTION_CHARS or item not in RAW_INDEXABLE:
            continue
        sections.append(FilingSection(item=item, title=title, text=body))
    return sections


def parse_raw_filing(path: Path) -> Filing | None:
    """Parse a raw ``.txt``/``.htm`` filing pulled directly from EDGAR."""
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        log.warning("rag.unreadable_filing", path=str(path), error=str(exc))
        return None

    header = {}
    for key, pattern in _HEADER_FIELDS.items():
        match = re.search(pattern, raw[:8000], re.I)
        header[key] = match.group(1).strip() if match else None

    form_type = (header.get("form_type") or "").upper()
    if form_type and not form_type.startswith("10-K"):
        return None

    sections = _split_sections(_strip_markup(raw))
    if not sections:
        log.warning("rag.no_sections_found", path=path.name)
        return None

    period = header.get("period")
    # Kaggle-style names look like 789019_10K_2021_0001564590-21-039151.htm.
    name_match = re.match(r"(\d+)_10K_(\d{4})_", path.stem)

    return Filing(
        source_path=path,
        company=header.get("company") or path.stem.replace("_", " ").title(),
        cik=header.get("cik") or (name_match.group(1) if name_match else None),
        ticker=(header.get("ticker") or "").upper() or None,
        form_type=form_type or "10-K",
        fiscal_year=(period[:4] if period else (name_match.group(2) if name_match else None)),
        filed_date=header.get("filed_date"),
        sections=sections,
    )


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def load_filings(*, limit: int | None = None, raw_dir: Path | None = None) -> list[Filing]:
    """Parse available 10-K filings, richest first.

    ``limit`` caps how many are indexed. It defaults to ``RAG_FILING_LIMIT``
    because the full Kaggle corpus is ~190 filings averaging ~160k characters of
    indexable prose — tens of thousands of chunks, which is a long CPU-only
    embedding job for no extra demonstrative value.

    Ordering puts ``RAG_PREFERRED_COMPANIES`` first — the companies that also
    have price history in the streamed universe, so a single question can span
    the lakehouse and the filing corpus — then fills the remaining slots with
    whichever filings carry the most indexable text.
    """
    raw_dir = raw_dir or settings.data_root / "raw"
    limit = limit if limit is not None else settings.rag_filing_limit

    filings: list[Filing] = []
    for sidecar in discover_sidecars(raw_dir):
        parsed = parse_sidecar(sidecar)
        if parsed is not None:
            filings.append(parsed)

    if filings:
        log.info("rag.sidecar_layout_detected", filings=len(filings))
    else:
        # No sidecars: fall back to parsing raw filings directly.
        candidates = [
            path
            for path in raw_dir.rglob("*")
            if path.is_file()
            and path.suffix.lower() in RAW_FILING_SUFFIXES
            and path.stat().st_size > 20_000
        ]
        log.info("rag.raw_layout_detected", candidates=len(candidates))
        for path in sorted(candidates, key=lambda p: p.stat().st_size, reverse=True)[
            : (limit or len(candidates))
        ]:
            parsed = parse_raw_filing(path)
            if parsed is not None:
                filings.append(parsed)

    if not filings:
        raise FileNotFoundError(
            f"No parseable 10-K filings found under {raw_dir}. Place the Kaggle "
            "SEC EDGAR download (or raw filings from https://www.sec.gov/edgar) "
            "there and re-run."
        )

    preferred = settings.preferred_companies

    def is_preferred(filing: Filing) -> bool:
        name = filing.company.upper()
        return any(candidate in name for candidate in preferred)

    # Preferred first, then richest. Negating the boolean sorts True ahead.
    filings.sort(key=lambda f: (not is_preferred(f), -f.indexable_chars))
    if limit:
        filings = filings[:limit]

    log.info(
        "rag.filings_loaded",
        count=len(filings),
        preferred_included=sum(1 for f in filings if is_preferred(f)),
        total_indexable_chars=sum(f.indexable_chars for f in filings),
        companies=[f.company for f in filings[:10]],
    )
    return filings


def iter_sections(filings: Iterable[Filing]):
    for filing in filings:
        for section in filing.sections:
            yield filing, section
