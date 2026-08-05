"""Building the two indexes hybrid retrieval searches over.

Dense and sparse retrieval fail in different places, which is exactly why the
rubric asks for both:

* **Dense** (Chroma + ``bge-small-en-v1.5``) matches meaning. It finds a passage
  about "disruption to our component suppliers" for the query "supply chain
  risk" even though they share almost no words.
* **Sparse** (BM25 over the same chunks) matches terms. It reliably finds the
  passage containing a literal ``CIK``, a product name or a defined term — the
  cases where an embedding's smoothing loses the signal.

Both indexes are written from the *same* chunk list so their IDs align and
:mod:`fmis.rag.retrieve` can fuse the two rankings.

``bge`` models are trained with an asymmetric instruction: queries carry a
prefix, passages do not. Getting that wrong silently degrades recall, so the
prefix lives here as a constant shared with the retriever.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from fmis.config import settings
from fmis.lineage import dataset, lineage_run
from fmis.logging_setup import configure_logging
from fmis.rag.chunker import Chunk, chunk_filings
from fmis.rag.loader import load_filings

log = configure_logging("rag_index")

COLLECTION_NAME = "sec_filings"

# Required by the bge family for retrieval; passages are embedded bare.
BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "


@lru_cache(maxsize=2)
def get_embedder(model_name: str | None = None):
    """Load the sentence-transformer once per process (it is ~130 MB)."""
    from sentence_transformers import SentenceTransformer

    name = model_name or settings.rag_embedding_model
    log.info("rag.loading_embedder", model=name)
    return SentenceTransformer(name, device="cpu")


def embed_passages(texts: list[str], *, batch_size: int = 32) -> list[list[float]]:
    model = get_embedder()
    vectors = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        normalize_embeddings=True,  # cosine similarity via dot product
        convert_to_numpy=True,
    )
    return vectors.tolist()


def embed_query(query: str) -> list[float]:
    model = get_embedder()
    vector = model.encode(
        [BGE_QUERY_PREFIX + query],
        show_progress_bar=False,
        normalize_embeddings=True,
        convert_to_numpy=True,
    )
    return vector[0].tolist()


def get_chroma_collection(*, reset: bool = False):
    """Open (or recreate) the persistent Chroma collection."""
    import chromadb
    from chromadb.config import Settings as ChromaSettings

    path = settings.chroma_path
    if reset and path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)

    client = chromadb.PersistentClient(
        path=str(path),
        settings=ChromaSettings(anonymized_telemetry=False, allow_reset=True),
    )
    return client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


@dataclass
class IndexStats:
    filings: int
    chunks: int
    vector_dimensions: int
    collection_count: int
    bm25_corpus_path: str
    chroma_path: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "filings": self.filings,
            "chunks": self.chunks,
            "vector_dimensions": self.vector_dimensions,
            "collection_count": self.collection_count,
            "bm25_corpus_path": self.bm25_corpus_path,
            "chroma_path": self.chroma_path,
        }


def _write_bm25_corpus(chunks: list[Chunk]) -> None:
    """Persist chunk text + metadata for the sparse index.

    rank_bm25 has no on-disk format, so the corpus is stored as JSONL and the
    index is rebuilt in memory at query time — cheap at this corpus size and it
    keeps the two indexes provably in sync.
    """
    target = settings.bm25_path
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for chunk in chunks:
            handle.write(
                json.dumps({"chunk_id": chunk.chunk_id, "text": chunk.text, **chunk.as_metadata()})
                + "\n"
            )
    log.info("rag.bm25_corpus_written", path=str(target), chunks=len(chunks))


def build_index(*, reset: bool = True, filing_limit: int | None = None) -> dict[str, Any]:
    """Parse, chunk, embed and index every filing under ``data/raw``."""
    settings.ensure_directories()

    with lineage_run(
        "rag_index_build",
        inputs=[dataset(f"file://{settings.data_root / 'raw'}")],
        outputs=[
            dataset(f"chroma://{settings.chroma_path}/{COLLECTION_NAME}"),
            dataset(f"file://{settings.bm25_path}"),
        ],
    ) as run:
        filings = load_filings(limit=filing_limit)
        chunks = chunk_filings(filings)

        collection = get_chroma_collection(reset=reset)

        vectors = embed_passages([c.text for c in chunks])
        collection.upsert(
            ids=[c.chunk_id for c in chunks],
            embeddings=vectors,
            documents=[c.text for c in chunks],
            metadatas=[c.as_metadata() for c in chunks],
        )

        _write_bm25_corpus(chunks)

        stats = IndexStats(
            filings=len(filings),
            chunks=len(chunks),
            vector_dimensions=len(vectors[0]) if vectors else 0,
            collection_count=collection.count(),
            bm25_corpus_path=str(settings.bm25_path),
            chroma_path=str(settings.chroma_path),
        )
        run.record(**stats.as_dict())

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "embedding_model": settings.rag_embedding_model,
            "chunk_chars": settings.rag_chunk_chars,
            "chunk_overlap_chars": settings.rag_chunk_overlap_chars,
            "documents": [f.display_name for f in filings],
            **stats.as_dict(),
        }

        target: Path = settings.evidence_root / "runs" / "rag_index.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    log.info("rag.index_built", **stats.as_dict())
    return summary


def load_bm25_corpus() -> list[dict[str, Any]]:
    if not settings.bm25_path.exists():
        raise FileNotFoundError(
            f"No BM25 corpus at {settings.bm25_path}. Build the index first "
            "(`fmis rag-index`)."
        )
    with settings.bm25_path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


if __name__ == "__main__":  # pragma: no cover
    build_index()
