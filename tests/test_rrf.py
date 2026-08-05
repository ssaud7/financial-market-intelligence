"""Reciprocal Rank Fusion must actually reward cross-retriever agreement.

The point of RRF in this pipeline is that a passage both retrievers liked beats
a passage only one of them ranked first. These tests pin that property, plus the
bookkeeping the evidence trail depends on.
"""

from __future__ import annotations

import pytest

from fmis.rag.retrieve import Retrieved, reciprocal_rank_fusion


def dense(chunk_id: str, rank: int) -> Retrieved:
    return Retrieved(
        chunk_id=chunk_id,
        text=f"text for {chunk_id}",
        metadata={"citation_label": chunk_id},
        dense_rank=rank,
        dense_score=1.0 / rank,
        retrievers=["dense"],
    )


def sparse(chunk_id: str, rank: int) -> Retrieved:
    return Retrieved(
        chunk_id=chunk_id,
        text=f"text for {chunk_id}",
        metadata={"citation_label": chunk_id},
        sparse_rank=rank,
        sparse_score=10.0 / rank,
        retrievers=["sparse"],
    )


def test_agreement_beats_a_single_first_place():
    """B is #2 on both lists; A is #1 on one and absent from the other."""
    dense_hits = [dense("A", 1), dense("B", 2)]
    sparse_hits = [sparse("C", 1), sparse("B", 2)]

    fused = reciprocal_rank_fusion([dense_hits, sparse_hits], k=60)

    assert fused[0].chunk_id == "B", "a passage both retrievers ranked highly should win"
    assert fused[0].retrievers == ["dense", "sparse"]


def test_score_matches_the_formula():
    """Rank is the passage's position in the list handed to the fuser."""
    dense_hits = [dense("X", 1), dense("A", 2)]
    sparse_hits = [sparse("Y", 1), sparse("Z", 2), sparse("A", 3)]

    fused = reciprocal_rank_fusion([dense_hits, sparse_hits], k=60)
    scored = {h.chunk_id: h.rrf_score for h in fused}

    assert scored["A"] == pytest.approx(1 / (60 + 2) + 1 / (60 + 3))


def test_fusion_uses_list_position_not_the_carried_retriever_rank():
    """The two can legitimately differ, and position is the honest rank.

    BM25 search drops zero-scoring hits after enumerating, so a passage can
    carry ``sparse_rank=9`` while sitting third in the returned list. Only the
    returned list reflects what is actually being fused.
    """
    hit = sparse("A", 9)  # claims rank 9...
    fused = reciprocal_rank_fusion([[hit]], k=60)  # ...but is first in the list

    assert fused[0].rrf_score == pytest.approx(1 / (60 + 1))
    assert fused[0].sparse_rank == 9, "the retriever's own rank is still recorded"


def test_single_retriever_ordering_is_preserved():
    fused = reciprocal_rank_fusion([[dense("A", 1), dense("B", 2), dense("C", 3)]], k=60)
    assert [h.chunk_id for h in fused] == ["A", "B", "C"]


def test_per_retriever_ranks_survive_fusion():
    """The evidence trail needs to show how each retriever ranked a passage."""
    fused = reciprocal_rank_fusion([[dense("A", 4)], [sparse("A", 7)]], k=60)
    hit = fused[0]
    assert hit.dense_rank == 4
    assert hit.sparse_rank == 7
    assert hit.dense_score is not None and hit.sparse_score is not None


def test_final_rank_is_assigned_contiguously():
    fused = reciprocal_rank_fusion([[dense("A", 1), dense("B", 2)], [sparse("C", 1)]], k=60)
    assert [h.final_rank for h in fused] == [1, 2, 3]


def test_smaller_k_sharpens_the_top_of_a_list():
    """k damps the head of each ranking: a small k widens the #1 vs #2 gap."""
    sharp = reciprocal_rank_fusion([[dense("A", 1), dense("B", 2)]], k=1)
    flat = reciprocal_rank_fusion([[dense("A", 1), dense("B", 2)]], k=1000)

    sharp_gap = sharp[0].rrf_score - sharp[1].rrf_score
    flat_gap = flat[0].rrf_score - flat[1].rrf_score
    assert sharp_gap > flat_gap


def test_large_k_lets_agreement_outweigh_a_single_top_rank():
    """This is why k=60 is the default rather than something near zero.

    With a large k the head of each list is damped, so a passage ranked #2 by
    both retrievers overtakes one ranked #1 by a single retriever. With a small
    k the single #1 wins outright — the ranking is no longer really a fusion.
    """
    dense_hits = [dense("SOLO", 1), dense("BOTH", 2)]
    sparse_hits = [sparse("OTHER", 1), sparse("BOTH", 2)]

    fused_large_k = reciprocal_rank_fusion(
        [list(dense_hits), list(sparse_hits)], k=60
    )
    assert fused_large_k[0].chunk_id == "BOTH"

    for hit in dense_hits + sparse_hits:
        hit.rrf_score = 0.0
    fused_small_k = reciprocal_rank_fusion(
        [list(dense_hits), list(sparse_hits)], k=0
    )
    assert fused_small_k[0].chunk_id == "SOLO"


def test_empty_input_is_not_an_error():
    assert reciprocal_rank_fusion([[], []], k=60) == []
