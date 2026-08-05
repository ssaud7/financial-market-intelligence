"""Hybrid retrieval: dense + BM25, fused with Reciprocal Rank Fusion.

Dense and sparse retrievers return scores on incomparable scales — a cosine
similarity of 0.82 and a BM25 score of 14.3 cannot be added, averaged, or
thresholded together, and normalising them is fragile because BM25's range
shifts with corpus and query length.

**Reciprocal Rank Fusion** sidesteps the problem by discarding the scores and
fusing the *ranks*:

    RRF(d) = Σ over retrievers r of   1 / (k + rank_r(d))

with ``k = 60`` (the value from the original Cormack et al. paper, and the
project default). Two properties make it the right tool here:

* It is scale-free, so no normalisation or tuning per retriever is needed.
* The constant ``k`` damps the top of each list, so a document ranked #1 by one
  retriever does not automatically beat a document ranked #2 by *both*. Passages
  that satisfy semantic *and* lexical relevance rise to the top — which is
  precisely the behaviour hybrid search is supposed to buy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from fmis.config import settings
from fmis.logging_setup import configure_logging
from fmis.rag.index import embed_query, get_chroma_collection, load_bm25_corpus

log = configure_logging("rag_retrieve")

_TOKEN = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    return _TOKEN.findall(text.lower())


@dataclass
class Retrieved:
    """A candidate passage and how each retriever ranked it."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]
    dense_rank: int | None = None
    dense_score: float | None = None
    sparse_rank: int | None = None
    sparse_score: float | None = None
    rrf_score: float = 0.0
    rerank_score: float | None = None
    final_rank: int | None = None
    retrievers: list[str] = field(default_factory=list)

    @property
    def citation_label(self) -> str:
        return self.metadata.get("citation_label", self.metadata.get("source_file", "unknown"))

    def as_dict(self, *, include_text: bool = False) -> dict[str, Any]:
        payload = {
            "chunk_id": self.chunk_id,
            "citation_label": self.citation_label,
            "retrievers": self.retrievers,
            "dense_rank": self.dense_rank,
            "sparse_rank": self.sparse_rank,
            "rrf_score": round(self.rrf_score, 6),
            "rerank_score": (
                round(self.rerank_score, 4) if self.rerank_score is not None else None
            ),
            "final_rank": self.final_rank,
        }
        if include_text:
            payload["text"] = self.text
        return payload


# --------------------------------------------------------------------------
# Individual retrievers
# --------------------------------------------------------------------------

def dense_search(query: str, top_k: int | None = None) -> list[Retrieved]:
    """Vector search over the Chroma collection."""
    top_k = top_k or settings.rag_dense_top_k
    collection = get_chroma_collection()

    result = collection.query(
        query_embeddings=[embed_query(query)],
        n_results=min(top_k, max(collection.count(), 1)),
        include=["documents", "metadatas", "distances"],
    )

    hits: list[Retrieved] = []
    ids = result["ids"][0]
    for rank, chunk_id in enumerate(ids, start=1):
        distance = result["distances"][0][rank - 1]
        hits.append(
            Retrieved(
                chunk_id=chunk_id,
                text=result["documents"][0][rank - 1],
                metadata=result["metadatas"][0][rank - 1] or {},
                dense_rank=rank,
                # Chroma reports cosine *distance*; similarity is 1 - distance.
                dense_score=round(1.0 - float(distance), 6),
                retrievers=["dense"],
            )
        )

    log.info("rag.dense_search", query=query, hits=len(hits))
    return hits


@lru_cache(maxsize=1)
def _bm25_index():
    """Build the BM25 index once per process from the persisted corpus."""
    from rank_bm25 import BM25Okapi

    corpus = load_bm25_corpus()
    tokenized = [tokenize(record["text"]) for record in corpus]
    log.info("rag.bm25_built", documents=len(corpus))
    return BM25Okapi(tokenized), corpus


def sparse_search(query: str, top_k: int | None = None) -> list[Retrieved]:
    """BM25 keyword search over the same chunks."""
    top_k = top_k or settings.rag_sparse_top_k
    bm25, corpus = _bm25_index()

    scores = bm25.get_scores(tokenize(query))
    ordered = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]

    hits: list[Retrieved] = []
    for rank, position in enumerate(ordered, start=1):
        # A zero BM25 score means no query term appeared; it carries no signal
        # and would only add noise to the fusion.
        if scores[position] <= 0:
            continue
        record = dict(corpus[position])
        text = record.pop("text")
        chunk_id = record.pop("chunk_id")
        hits.append(
            Retrieved(
                chunk_id=chunk_id,
                text=text,
                metadata=record,
                sparse_rank=rank,
                sparse_score=round(float(scores[position]), 6),
                retrievers=["sparse"],
            )
        )

    log.info("rag.sparse_search", query=query, hits=len(hits))
    return hits


# --------------------------------------------------------------------------
# Fusion
# --------------------------------------------------------------------------

def reciprocal_rank_fusion(
    rankings: list[list[Retrieved]],
    *,
    k: int | None = None,
) -> list[Retrieved]:
    """Fuse several ranked lists into one by summing 1 / (k + rank)."""
    k = k if k is not None else settings.rag_rrf_k
    merged: dict[str, Retrieved] = {}

    for ranking in rankings:
        for position, hit in enumerate(ranking, start=1):
            existing = merged.get(hit.chunk_id)
            if existing is None:
                existing = Retrieved(
                    chunk_id=hit.chunk_id,
                    text=hit.text,
                    metadata=hit.metadata,
                    retrievers=[],
                )
                merged[hit.chunk_id] = existing

            # Preserve each retriever's own rank/score for the evidence trail.
            if hit.dense_rank is not None:
                existing.dense_rank = hit.dense_rank
                existing.dense_score = hit.dense_score
            if hit.sparse_rank is not None:
                existing.sparse_rank = hit.sparse_rank
                existing.sparse_score = hit.sparse_score

            for retriever in hit.retrievers:
                if retriever not in existing.retrievers:
                    existing.retrievers.append(retriever)

            existing.rrf_score += 1.0 / (k + position)

    fused = sorted(merged.values(), key=lambda h: h.rrf_score, reverse=True)
    for rank, hit in enumerate(fused, start=1):
        hit.final_rank = rank

    log.info(
        "rag.rrf_fused",
        k=k,
        candidates=len(fused),
        found_by_both=sum(1 for h in fused if len(h.retrievers) > 1),
    )
    return fused


def hybrid_search(
    query: str,
    *,
    dense_k: int | None = None,
    sparse_k: int | None = None,
    rrf_k: int | None = None,
) -> list[Retrieved]:
    """Run both retrievers and return the RRF-fused candidate list."""
    dense = dense_search(query, dense_k)
    sparse = sparse_search(query, sparse_k)
    return reciprocal_rank_fusion([dense, sparse], k=rrf_k)
