"""Stage 3 — Retrieval-Augmented Generation over SEC 10-K filings.

``loader``    parse filings into section-aware documents (Item 1A, Item 7, ...)
``chunker``   boundary-respecting overlapping chunks that carry their provenance
``index``     Chroma vector store + BM25 corpus, built from the same chunks
``retrieve``  dense and sparse search fused with Reciprocal Rank Fusion
``rerank``    cross-encoder reranking of the fused candidates
``answer``    grounded generation with inline citations, on a local Ollama model
``pipeline``  the full chain, with a recorded evidence trail per query
"""

from fmis.rag.index import build_index
from fmis.rag.pipeline import answer_question, run_demo

__all__ = ["build_index", "answer_question", "run_demo"]
