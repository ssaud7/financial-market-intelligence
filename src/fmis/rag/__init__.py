"""Stage 3 — Retrieval-Augmented Generation over SEC 10-K filings.

``loader``    parse filings into section-aware documents (Item 1A, Item 7, ...)
``chunker``   boundary-respecting overlapping chunks that carry their provenance
``index``     Chroma vector store + BM25 corpus, built from the same chunks
``retrieve``  dense and sparse search fused with Reciprocal Rank Fusion
``rerank``    cross-encoder reranking of the fused candidates
``answer``    grounded generation with inline citations, on a local Ollama model
``pipeline``  the full chain, with a recorded evidence trail per query

Nothing is re-exported here on purpose. ``index`` pulls in ChromaDB and
sentence-transformers, so an eager re-export would make ``import
fmis.rag.chunker`` — or any CLI command that merely mentions the package — pay
several seconds of import cost for a dependency it does not use. Import from the
submodule you actually need.
"""
