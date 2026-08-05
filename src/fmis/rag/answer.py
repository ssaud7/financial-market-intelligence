"""Grounded answer generation against a local Ollama model.

Everything here exists to make one property true: **the answer may contain only
claims supported by the retrieved passages**, and each claim must point at the
passage supporting it.

Three mechanisms enforce that, because prompting alone does not:

1. **The prompt** numbers each passage ``[S1] … [Sn]``, forbids outside
   knowledge, and mandates an explicit "the filings provided do not contain
   this" when the context is insufficient.
2. **Citation extraction** parses the ``[Sn]`` markers back out of the answer
   and resolves them to the source chunks, so a reader gets company, fiscal
   year, Item number and file name for every marker.
3. **Post-hoc verification** flags an answer that cited nothing, or that cited a
   source index that was never supplied — the observable signature of a
   hallucinated citation. The flags travel with the result rather than being
   silently swallowed.

Generation runs on a local Ollama server, so the stage needs no API key and
makes no outbound calls once the model is pulled.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

from fmis.config import settings
from fmis.logging_setup import configure_logging
from fmis.rag.retrieve import Retrieved

log = configure_logging("rag_answer")

CITATION_MARKER = re.compile(r"\[S(\d+)\]")

SYSTEM_PROMPT = """You are a financial research analyst answering questions about \
public companies strictly from excerpts of their SEC Form 10-K annual reports.

Rules you must follow:

1. Use ONLY the numbered sources provided in the user message. You have no other \
knowledge of these companies. Do not use anything you may remember about them.
2. Cite the source for every factual claim using its marker, like [S1] or [S3]. A \
sentence drawing on two sources cites both: [S1][S4].
3. Never cite a source number that was not provided to you.
4. If the sources do not contain enough information to answer, say exactly: \
"The filings provided do not contain enough information to answer this question." \
Then state briefly what is missing. Do not guess and do not fill the gap from \
general knowledge.
5. Quote the filing's own wording for specific risks, figures and defined terms \
rather than paraphrasing them loosely.
6. Be direct and concise. Lead with the answer, then the supporting detail. Do \
not add disclaimers about not being a financial advisor.
"""

USER_TEMPLATE = """Answer the question using only the sources below.

{sources}

QUESTION: {question}

Answer with inline [S#] citations."""


@dataclass
class Citation:
    """One resolved ``[Sn]`` marker."""

    marker: str
    source_index: int
    chunk_id: str
    citation_label: str
    company: str
    ticker: str
    fiscal_year: str
    item: str
    source_file: str
    excerpt: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "marker": self.marker,
            "source_index": self.source_index,
            "chunk_id": self.chunk_id,
            "citation_label": self.citation_label,
            "company": self.company,
            "ticker": self.ticker,
            "fiscal_year": self.fiscal_year,
            "item": self.item,
            "source_file": self.source_file,
            "excerpt": self.excerpt,
        }


@dataclass
class GroundedAnswer:
    question: str
    answer: str
    citations: list[Citation]
    sources_supplied: list[dict[str, Any]]
    model: str
    elapsed_seconds: float
    warnings: list[str] = field(default_factory=list)
    refused: bool = False
    usage: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "question": self.question,
            "answer": self.answer,
            "refused": self.refused,
            "model": self.model,
            "elapsed_seconds": self.elapsed_seconds,
            "citations": [c.as_dict() for c in self.citations],
            "sources_supplied": self.sources_supplied,
            "warnings": self.warnings,
            "usage": self.usage,
        }


def _ollama_client():
    from ollama import Client

    return Client(host=settings.ollama_host, timeout=settings.ollama_timeout_s)


def ensure_model_available(model: str | None = None) -> str:
    """Confirm the generation model is pulled, with an actionable error if not."""
    model = model or settings.ollama_model
    client = _ollama_client()

    try:
        listed = client.list()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Cannot reach the Ollama server at {settings.ollama_host}. "
            "Start it with `docker compose up -d ollama`. Original error: "
            f"{exc}"
        ) from exc

    available = {
        entry.get("model") or entry.get("name", "")
        for entry in (listed.get("models") or [])
    }
    # Ollama reports tagless pulls as "name:latest".
    if model in available or f"{model}:latest" in available:
        return model

    raise RuntimeError(
        f"Ollama model {model!r} is not pulled. Run `make ollama-pull` "
        f"(or `docker compose exec ollama ollama pull {model}`). "
        f"Currently available: {sorted(available) or 'none'}"
    )


def build_source_block(passages: list[Retrieved]) -> tuple[str, list[dict[str, Any]]]:
    """Render the numbered source block and the manifest describing it."""
    lines: list[str] = []
    manifest: list[dict[str, Any]] = []

    for index, passage in enumerate(passages, start=1):
        meta = passage.metadata
        label = passage.citation_label
        lines.append(
            f"[S{index}] {label}\n"
            f"(company: {meta.get('company', 'unknown')}, "
            f"fiscal year: {meta.get('fiscal_year', 'unknown')}, "
            f"file: {meta.get('source_file', 'unknown')})\n"
            f"{passage.text.strip()}\n"
        )
        manifest.append(
            {
                "source_index": index,
                "chunk_id": passage.chunk_id,
                "citation_label": label,
                "company": meta.get("company", ""),
                "ticker": meta.get("ticker", ""),
                "fiscal_year": meta.get("fiscal_year", ""),
                "item": meta.get("item", ""),
                "source_file": meta.get("source_file", ""),
                "rerank_score": passage.rerank_score,
                "rrf_score": round(passage.rrf_score, 6),
                "retrievers": passage.retrievers,
            }
        )

    return "\n".join(lines), manifest


def extract_citations(answer: str, passages: list[Retrieved]) -> tuple[list[Citation], list[str]]:
    """Resolve ``[Sn]`` markers to sources; report markers that resolve to nothing."""
    citations: list[Citation] = []
    warnings: list[str] = []
    seen: set[int] = set()

    for match in CITATION_MARKER.finditer(answer):
        index = int(match.group(1))
        if index in seen:
            continue
        seen.add(index)

        if not 1 <= index <= len(passages):
            # The model invented a source number. This is the observable
            # signature of a fabricated citation, so it is surfaced, not hidden.
            warnings.append(
                f"Answer cites [S{index}], but only {len(passages)} sources were supplied."
            )
            continue

        passage = passages[index - 1]
        meta = passage.metadata
        citations.append(
            Citation(
                marker=f"[S{index}]",
                source_index=index,
                chunk_id=passage.chunk_id,
                citation_label=passage.citation_label,
                company=meta.get("company", ""),
                ticker=meta.get("ticker", ""),
                fiscal_year=meta.get("fiscal_year", ""),
                item=meta.get("item", ""),
                source_file=meta.get("source_file", ""),
                excerpt=passage.text.strip()[:400],
            )
        )

    return citations, warnings


def generate_answer(
    question: str,
    passages: list[Retrieved],
    *,
    model: str | None = None,
) -> GroundedAnswer:
    """Ask the local model to answer strictly from ``passages``."""
    model = model or settings.ollama_model
    started = time.monotonic()

    if not passages:
        return GroundedAnswer(
            question=question,
            answer=(
                "The filings provided do not contain enough information to answer "
                "this question. Retrieval returned no passages for this query."
            ),
            citations=[],
            sources_supplied=[],
            model=model,
            elapsed_seconds=0.0,
            warnings=["Retrieval returned zero passages; generation was skipped."],
            refused=True,
        )

    source_block, manifest = build_source_block(passages)
    user_message = USER_TEMPLATE.format(sources=source_block, question=question)

    client = _ollama_client()
    response = client.chat(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        options={
            # Deterministic decoding: a cited, grounded answer should not vary
            # between runs, and fixed output makes the evidence reproducible.
            "temperature": settings.ollama_temperature,
            "seed": settings.ollama_seed,
            # Ollama defaults to a small context window; the source block alone
            # is several thousand tokens, and silently truncating it would drop
            # the passages the answer is supposed to be grounded in.
            "num_ctx": 8192,
            "num_predict": settings.rag_max_answer_tokens,
        },
    )

    answer_text = (response.get("message") or {}).get("content", "").strip()
    elapsed = time.monotonic() - started

    citations, warnings = extract_citations(answer_text, passages)
    refused = "do not contain enough information" in answer_text.lower()

    if not citations and not refused:
        warnings.append(
            "Answer contains no [S#] citations despite passages being supplied — "
            "treat it as ungrounded."
        )

    usage = {
        "prompt_eval_count": response.get("prompt_eval_count"),
        "eval_count": response.get("eval_count"),
        "total_duration_ms": (
            round(response["total_duration"] / 1e6) if response.get("total_duration") else None
        ),
        "tokens_per_second": (
            round(response["eval_count"] / (response["eval_duration"] / 1e9), 2)
            if response.get("eval_count") and response.get("eval_duration")
            else None
        ),
    }

    log.info(
        "rag.answer_generated",
        question=question,
        model=model,
        sources=len(passages),
        citations=len(citations),
        refused=refused,
        warnings=warnings,
        elapsed_seconds=round(elapsed, 2),
        **{k: v for k, v in usage.items() if v is not None},
    )

    return GroundedAnswer(
        question=question,
        answer=answer_text,
        citations=citations,
        sources_supplied=manifest,
        model=model,
        elapsed_seconds=round(elapsed, 2),
        warnings=warnings,
        refused=refused,
        usage=usage,
    )
