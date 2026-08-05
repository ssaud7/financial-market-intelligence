"""Financial Market Intelligence System.

An end-to-end data platform built for the SDAIA Academy capstone
"Modern Data Engineering for AI Systems". Five stages, each using the real
library rather than a stand-in:

1. ``fmis.ingestion``  — Kafka producer/consumer with a Pydantic data contract
                          enforced at the boundary and a dead-letter path.
2. ``fmis.lakehouse``  — Delta Lake Bronze/Silver/Gold, with a MERGE upsert
                          keyed on the ticker business key.
3. ``fmis.rag``        — chunking, embeddings, Chroma + BM25 hybrid retrieval
                          fused with RRF, cross-encoder reranking, cited answers.
4. ``fmis.quality`` /
   ``fmis.lineage``    — Great Expectations gates and OpenLineage run events.
5. ``dags/``           — the Airflow DAG that wires it all together.
"""

__version__ = "1.0.0"
