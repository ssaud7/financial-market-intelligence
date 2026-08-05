"""Central configuration, loaded from environment/.env via pydantic-settings.

Every stage imports ``settings`` from here rather than reading os.environ, so
there is exactly one place that defines where data lands, which topics are used,
and which models the RAG stage talks to.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# src/fmis/config.py -> src/fmis -> src -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Runtime configuration for all five pipeline stages."""

    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- Kafka -----------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_quotes: str = "market.quotes.raw"
    kafka_topic_dlq: str = "market.quotes.dlq"
    kafka_consumer_group: str = "fmis-bronze-loader"

    # ---- Paths -----------------------------------------------------------
    data_root: Path = Path("data")
    lakehouse_root: Path = Path("data/lakehouse")
    landing_root: Path = Path("data/landing")
    quarantine_root: Path = Path("data/quarantine")
    vectorstore_root: Path = Path("data/vectorstore")

    # ---- Ingestion -------------------------------------------------------
    producer_corruption_rate: float = Field(default=0.08, ge=0.0, le=1.0)
    producer_max_records: int = Field(default=25_000, gt=0)
    producer_tickers: str = "AAPL,MSFT,NVDA,AMZN,GOOGL,META,JPM,XOM,UNH,PG"

    # ---- RAG -------------------------------------------------------------
    anthropic_api_key: str = ""
    rag_generation_model: str = "claude-sonnet-5"
    rag_embedding_model: str = "BAAI/bge-small-en-v1.5"
    rag_rerank_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    rag_chunk_tokens: int = 900
    rag_chunk_overlap: int = 150
    rag_dense_top_k: int = 30
    rag_sparse_top_k: int = 30
    rag_rrf_k: int = 60
    rag_rerank_top_n: int = 6

    # ---- OpenLineage -----------------------------------------------------
    openlineage_transport: str = "file"
    openlineage_file: Path = Path("evidence/lineage/openlineage-events.jsonl")
    openlineage_url: str = "http://localhost:5000"
    openlineage_namespace: str = "financial-market-intelligence"

    @field_validator(
        "data_root",
        "lakehouse_root",
        "landing_root",
        "quarantine_root",
        "vectorstore_root",
        "openlineage_file",
        mode="after",
    )
    @classmethod
    def _absolutise(cls, value: Path) -> Path:
        """Resolve relative paths against the repo root, not the caller's cwd.

        Airflow invokes stages from arbitrary working directories; without this
        the DAG would silently write its lakehouse somewhere else than a manual
        ``fmis`` run does.
        """
        return value if value.is_absolute() else (REPO_ROOT / value).resolve()

    # ---- Derived ---------------------------------------------------------
    @property
    def tickers(self) -> list[str]:
        return [t.strip().upper() for t in self.producer_tickers.split(",") if t.strip()]

    @property
    def bronze_path(self) -> Path:
        return self.lakehouse_root / "bronze" / "quotes"

    @property
    def silver_path(self) -> Path:
        return self.lakehouse_root / "silver" / "quotes"

    @property
    def gold_path(self) -> Path:
        return self.lakehouse_root / "gold" / "ticker_metrics"

    @property
    def landing_valid_path(self) -> Path:
        return self.landing_root / "valid"

    @property
    def quarantine_path(self) -> Path:
        return self.quarantine_root / "quotes"

    @property
    def chroma_path(self) -> Path:
        return self.vectorstore_root / "chroma"

    @property
    def bm25_path(self) -> Path:
        return self.vectorstore_root / "bm25_corpus.jsonl"

    @property
    def evidence_root(self) -> Path:
        return REPO_ROOT / "evidence"

    def ensure_directories(self) -> None:
        """Create every directory the pipeline writes to. Idempotent."""
        for path in (
            self.landing_valid_path,
            self.quarantine_path,
            self.lakehouse_root,
            self.vectorstore_root,
            self.openlineage_file.parent,
            self.evidence_root / "logs",
            self.evidence_root / "runs",
        ):
            path.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
