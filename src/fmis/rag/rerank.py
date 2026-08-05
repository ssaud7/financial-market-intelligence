"""Cross-encoder reranking of the fused candidate list.

Retrieval and reranking answer different questions, and the difference is
architectural rather than a matter of degree.

The **bi-encoder** used for dense retrieval embeds the query and every passage
*independently*, then compares vectors. That independence is what makes it fast
enough to search thousands of chunks — the passage vectors were computed at
index time — but it also means the model never sees the query and the passage
together, so it cannot reason about how they relate.

The **cross-encoder** used here concatenates the query and one passage into a
single input and runs the full transformer over the pair, attending across both.
It is far more accurate at judging relevance and far too slow to run over the
whole corpus, so it is applied only to the handful of candidates RRF surfaced:
retrieve wide and cheap, then rerank narrow and expensive.

This is also the stage that removes RRF's blind spot. Fusion rewards passages
that *both* retrievers liked, which is usually right but promotes passages that
merely share vocabulary with the query. The cross-encoder demotes those.
"""

from __future__ import annotations

from functools import lru_cache

from fmis.config import settings
from fmis.logging_setup import configure_logging
from fmis.rag.retrieve import Retrieved

log = configure_logging("rag_rerank")


@lru_cache(maxsize=2)
def get_cross_encoder(model_name: str | None = None):
    from sentence_transformers import CrossEncoder

    name = model_name or settings.rag_rerank_model
    log.info("rag.loading_cross_encoder", model=name)
    # max_length caps the query+passage pair; ms-marco MiniLM was trained at 512.
    return CrossEncoder(name, max_length=512, device="cpu")


def rerank(
    query: str,
    candidates: list[Retrieved],
    *,
    top_n: int | None = None,
    batch_size: int = 16,
) -> list[Retrieved]:
    """Score every candidate against the query and return the best ``top_n``."""
    top_n = top_n or settings.rag_rerank_top_n

    if not candidates:
        log.warning("rag.rerank_no_candidates", query=query)
        return []

    model = get_cross_encoder()
    pairs = [(query, candidate.text) for candidate in candidates]
    scores = model.predict(pairs, batch_size=batch_size, show_progress_bar=False)

    for candidate, score in zip(candidates, scores):
        candidate.rerank_score = float(score)

    reranked = sorted(candidates, key=lambda c: c.rerank_score or float("-inf"), reverse=True)
    selected = reranked[:top_n]

    # Renumber so final_rank reflects the order actually handed to the LLM.
    for rank, candidate in enumerate(selected, start=1):
        candidate.final_rank = rank

    rrf_order = [c.chunk_id for c in sorted(candidates, key=lambda c: -c.rrf_score)][:top_n]
    moved = sum(1 for c in selected if c.chunk_id not in rrf_order)

    log.info(
        "rag.reranked",
        query=query,
        candidates=len(candidates),
        selected=len(selected),
        promoted_over_rrf=moved,
        top_score=round(selected[0].rerank_score, 4) if selected else None,
    )
    return selected
